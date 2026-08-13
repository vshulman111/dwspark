/*
--	Copyright (c) 2007 - 2021 by Victor Shulman.
--	Written by Victor Shulman.  Not derived from licensed software.
--
--	Permission is granted to anyone to use this software for any
--	purpose on any computer system, and to redistribute it in any way,
--	subject to the following restrictions:
--
--	1. The author is not responsible for the consequences of use of
--		this software, no matter how awful, even if they arise
--		from defects in it.
--
--	2. The origin of this software must not be misrepresented, either
--		by explicit claim or by omission.
--
--	3. This notice must not be removed or altered.
*/
package com.dbtimes.dw.common

import com.dbtimes.dw.common.DbmsConstants._

import java.text.SimpleDateFormat
import java.text.DateFormat
import java.util.Calendar
import java.util.{Date, Properties}
import com.typesafe.config.{Config, ConfigValue}
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.functions.{col, max, udf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.util.matching.Regex
import com.dbtimes.dw.common.MiscHelper.getDateFormatted

private[dw] object DataFrameHelper {

  def readDfFromSqlServer(
      userAsOption: Option[String],
      passwordAsOption: Option[String],
      connectionUriAsOption: Option[String],
      sourceTableAsOption: Option[String],
      partitionColumnAsOption: Option[String] = None,
      numberOfPartitions: Int = 0,
      dbmsSpecificAttributes: Map[String, String] = Map[String, String](),
      resultSchema: StructType
  ): DataFrame = {

    readFromJdbcDdms(userAsOption,
      passwordAsOption,
      connectionUriAsOption,
      sourceTableAsOption,
      partitionColumnAsOption,
      numberOfPartitions,
      dbmsSpecificAttributes,
      resultSchema,
      SqlServer)

  }

  def readDfFromOracle(
      userAsOption: Option[String],
      passwordAsOption: Option[String],
      connectionUriAsOption: Option[String],
      sourceTableAsOption: Option[String],
      partitionColumnAsOption: Option[String] = None,
      numberOfPartitions: Int = 0,
      dbmsSpecificAttributes: Map[String, String] = Map[String, String](),
      resultSchema: StructType
  ): DataFrame = {

    readFromJdbcDdms(userAsOption,
      passwordAsOption,
      connectionUriAsOption,
      sourceTableAsOption,
      partitionColumnAsOption,
      numberOfPartitions,
      dbmsSpecificAttributes,
      resultSchema,
      Oracle)
  }

  def readDfFromMongoDb(
      userAsOption: Option[String],
      passwordAsOption: Option[String],
      connectionUriAsOption: Option[String],
      sourceTableAsOption: Option[String],
      partitionColumnAsOption: Option[String] = None,
      numberOfPartitions: Int = 0,
      dbmsSpecificAttributes: Map[String, String] = Map[String, String](),
      resultSchema: StructType
  ): DataFrame = {
    val spark = SparkSession.builder().getOrCreate() // this gets previously created session

    // val dbmsAttributes = mutable.Map.from(dbmsSpecificAttributes)   scala 2.13
    val dbmsAttributes = mutable.Map(dbmsSpecificAttributes.toSeq: _* ) // Refactored for Scala 2.12

    // if username and password are defined and connection string does not have them, inssert them into connection string
    if (userAsOption.isDefined && passwordAsOption.isDefined && !connectionUriAsOption.get.contains('@')) {
      val splitOnTwoForwardSlaches = connectionUriAsOption.get.split("//")
      dbmsAttributes("connection.uri") = splitOnTwoForwardSlaches(0) + "//" + userAsOption.get + ":" + passwordAsOption.get + "@" + splitOnTwoForwardSlaches(1)
    }
    if (sourceTableAsOption.isDefined) // For Mongo DB sourceTable will have aggregation pipeline name if defined.
      dbmsAttributes("aggregation.pipeline") = sourceTableAsOption.get

    if (!resultSchema.isEmpty) {
      dbmsAttributes("schemaHints") = resultSchema.simpleString
    }

    spark.read
      .format("mongodb")
      .options(dbmsAttributes)
      .load()
  }

  private def readFromJdbcDdms(
      userAsOption: Option[String],
      passwordAsOption: Option[String],
      connectionUriAsOption: Option[String],
      sourceTableAsOption: Option[String],
      partitionColumnAsOption: Option[String] = None,
      numberOfPartitions: Int = 0,
      dbmsSpecificAttributes: Map[String, String] = Map[String, String](),
      resultSchema: StructType,
      dbms: DbmsVal): DataFrame = {

    val spark = SparkSession.builder().getOrCreate() // this gets previously created session

    val driverClass = dbms.driverClass
    Class.forName(driverClass) // This call just make sure the class is available later one. It is not otherwise needed

    // val dbmsAttributes = mutable.Map.from(dbmsSpecificAttributes)    // Scala 2.13
    val dbmsAttributes = mutable.Map(dbmsSpecificAttributes.toSeq: _*) // Refactored for Scala 2.12
    dbmsAttributes("url") = connectionUriAsOption.get
    dbmsAttributes("driver") = driverClass

    if (userAsOption.isDefined && passwordAsOption.isDefined) {
      dbmsAttributes("user") = userAsOption.get
      dbmsAttributes("password") = passwordAsOption.get
    }

    if (sourceTableAsOption.isDefined)
      dbmsAttributes("dbtable") = sourceTableAsOption.get

    // Action custom validator insures that missing lower or upper bounds can only be for integral data types
    if (partitionColumnAsOption.isDefined) {
      dbmsAttributes("partitionColumn") = partitionColumnAsOption.get
      dbmsAttributes("numPartitions") = numberOfPartitions.toString
    }

    if (partitionColumnAsOption.isDefined) {

      require(!resultSchema.isEmpty)
      val schemaForPartitionColumn = resultSchema(Set(partitionColumnAsOption.get))
      val partitionColumnTypeName = schemaForPartitionColumn.fields(0).dataType.typeName

      // Step 1. Determine upper and lower bounds if needed
      if (!dbmsAttributes.contains("lowerBound") || !dbmsAttributes.contains("upperBound")) {
        val dbmsAttributesForMinMax = dbmsAttributes.clone()

        // remove keys that are not applicable to finding min / max value
        dbmsAttributesForMinMax --= Set("lowerBound", "upperBound", "partitionColumn", "numPartitions")

        // Since partition column can only be defined in the schema we are guaranteed to have schema with at least one column
        dbmsAttributesForMinMax("customSchema") = schemaForPartitionColumn.toDDL

        val iterations = List("lowerBound", "upperBound")
        for (iteration <- iterations) {
          val baseStatement = if (iteration == "lowerBound")
            dbms.statementForMinValue
          else
            dbms.statementForMaxValue

          dbmsAttributesForMinMax("dbtable") = baseStatement
            .replace("<@PartitionColumn>", partitionColumnAsOption.get)
            .replace("<@SourceTable>", sourceTableAsOption.get)

          val minMax = spark.read.format("jdbc")
            .options(dbmsAttributesForMinMax)
            .load().head().get(0)

          if (minMax == null)
            throw new RuntimeException(s"""Loader ERROR: Cannot automatically set lower and upper bounds for partition column ${partitionColumnAsOption.get} as all values are NULL. Set "lowerBound" and "upperBound" in configuration and re-run the load""")

          dbmsAttributes(iteration) = minMax.toString
        }
      }


      // Step 2. Set "sessionInitStatement" for Oracle when partition column has types "timestamp" or "date" if not set
      if (dbms == Oracle && Set("timestamp", "date").contains(partitionColumnTypeName)) {
        val (formatName: String, formatValue: String) =
          if (partitionColumnTypeName == "timestamp")
            ("NLS_TIMESTAMP_FORMAT", "YYYY-MM-DD HH24:MI:SS.FF")
          else if (partitionColumnTypeName == "date")
            ("NLS_DATE_FORMAT", "YYYY-MM-DD HH24:MI:SS")
        val alterStatement = s"""ALTER SESSION SET ${formatName} = '${formatValue}'"""

        if (dbmsAttributes.contains("sessionInitStatement")) {
          val nlsPattern: Regex = s"""(?i)${formatName}\\s*=\\s*'{1,2}${formatValue.replace(".", "\\.")}'{1,2}""".r
          val errorMessage: String = s"""Loader ERROR: For partitioned read using ${partitionColumnAsOption.get} column add "$alterStatement" to existing "sessionInitStatement" field in "oracle" configuration """

          // If "sessionInitStatement" does not have expected FORMAT throw an exception
          val sessionInitStatement = dbmsAttributes("sessionInitStatement")
          if (!nlsPattern.findFirstIn(sessionInitStatement).isDefined)
            throw new RuntimeException(errorMessage)
        }
        else { // there no "sessionInitStatement" defined in configuration
          dbmsAttributes("sessionInitStatement") = alterStatement
        }
      }
    }

    if (!resultSchema.isEmpty) {
      dbmsAttributes("customSchema") = resultSchema.toDDL
    }

    spark.read.format("jdbc")
      .options(dbmsAttributes)
      .load()
  }

  /** *
   *
   * @param calendarStart
   * @param calendarEnd
   * @return data frame with calendar dates in the range from start to end inclusive
   *
   *         Sample usage:
   *         override def getCustomDatesToProcess(dfDatesToProcessBasedOnConfig: DataFrame): Option[DataFrame] = {
   *         val start =  new SimpleDateFormat("yyyy-MM-dd").parse( "2019-01-01")
   *         val calendarStart: Calendar = Calendar.getInstance()
   *         calendarStart.setTime(start)
   *         val calendarEnd: Calendar = Calendar.getInstance()  // current date
   *
   *         val dfDatesToProcess = DataFrameHelper.getCalendarDatesDataFrame( calendarStart, calendarEnd)
   *         Some(dfDatesToProcess)
   *         }
   */
  def getCalendarDatesDataFrame(calendarStart: Calendar, calendarEnd: Calendar): DataFrame = {
    val spark = SparkSession.builder().getOrCreate() // this gets previously created session
    val endDate = getDateFormatted(calendarEnd.getTime(), "yyyy-MM-dd")
    val startDate = getDateFormatted(calendarStart.getTime(), "yyyy-MM-dd")

    var dates: mutable.Seq[Row] = mutable.Seq.empty[Row]
    dates = dates :+ Row(startDate)
    if (endDate > startDate)
      do {
        calendarStart.add(Calendar.DATE, 1)
        val newDate = calendarStart.getTime()
        dates = dates :+ Row(getDateFormatted(newDate, "yyyy-MM-dd"))
        calendarStart.setTime(newDate)
      } while (endDate != getDateFormatted(calendarStart.getTime(), "yyyy-MM-dd"))

    val schema = StructType(Array(StructField("Date", StringType, true)))
    val rdd = spark.sparkContext.parallelize(dates.toVector) // convert  mutable.Seq[Row] to immutable[Seq] according to spark 4.1
    val dfDates = spark.createDataFrame(rdd, schema)
    val datesViewName = "CalendarDates" + java.util.UUID.randomUUID.toString.replace("-", "_")
    dfDates.createOrReplaceTempView(datesViewName)

    /* val sqlCalendarDates = s"""SELECT EXPLODE( SEQUENCE( TO_DATE( '2019-01-01'), TO_DATE( '2019-05-01'), INTERVAL 1 DAY ) """
    * */
    val sqlCalendarDates = s"""SELECT CAST( Date AS DATE ) AS CalendarDate FROM $datesViewName """
    val dfCalendarDates = spark.sql(sqlCalendarDates)
    dfCalendarDates
  }

  val concatColumns = udf((r: Row) => {
    val s = r.mkString("~")
    s
  })

  def compareFieldNamesOrder(schema1: StructType, schema2: StructType): Boolean = {
    val fields1 = schema1.fields.map(f => (f.name))
    val fields2 = schema2.fields.map(f => (f.name))

    fields1.sameElements(fields2)
  }

  def compareFieldNamesTypesOrder(schema1: StructType, schema2: StructType): Boolean = {
    val fields1 = schema1.fields.map(f => (f.name, f.dataType))
    val fields2 = schema2.fields.map(f => (f.name, f.dataType))

    fields1.sameElements(fields2)
  }

  /**
   * df source DataFrame
   */
  implicit class DataFrameImplicits(df: DataFrame) {

    /**
     * Set nullable property of column.
     *
     * @param colName  is the column name to change
     * @param nullable is the flag to set, such that the column is either nullable or not
     */
    def setNullableStateOfColumn(colName: String)(nullable: Boolean): DataFrame = {

      // get schema
      val schema = df.schema
      // modify [[StructField] with name `colName`
      val newSchema = StructType(schema.map {
        case StructField(c, t, _, m) if c.equals(colName) => StructField(c, t, nullable = nullable, m)
        case y: StructField => y
      })
      // apply new schema
      // df.sqlContext.createDataFrame(df.rdd, newSchema) // old way
      val spark = SparkSession.builder().getOrCreate() // this gets previously created session
      spark.createDataFrame(df.rdd, newSchema)
    }

    def setNullableStateForAllColumns(nullable: Boolean): DataFrame = {
      // get schema
      val schema = df.schema

      val allNullable: Boolean = schema.fields.forall(_.nullable)
      val allNotNullable: Boolean = schema.fields.forall(!_.nullable)

      if ( allNullable && nullable || allNotNullable && !nullable ) {
        df
      }
      else {
        // modify [[StructField] with name `cn`
        val newSchema = StructType(schema.map {
          case StructField(c, t, _, m) => StructField(c, t, nullable = nullable, m)
        })
        // apply new schema
        // df.sqlContext.createDataFrame(df.rdd, newSchema) // old way
        val spark = SparkSession.builder().getOrCreate() // this gets previously created session
        spark.createDataFrame(df.rdd, newSchema)
      }
    }

    def setNewSchema(newSchema: StructType): DataFrame = {
      // apply new schema
      // df.sqlContext.createDataFrame(df.rdd, newSchema) // old way
      val spark = SparkSession.builder().getOrCreate() // this gets previously created session
      spark.createDataFrame(df.rdd, newSchema)
    }

    def selectInCorrectOrderCastToCorrectTypeAndMakeNullable(newSchema: StructType): DataFrame = {
      val dfSchema: StructType = df.schema
      val allNullable: Boolean = dfSchema.fields.forall(_.nullable)
      if ( !compareFieldNamesTypesOrder( dfSchema, newSchema ) || !allNullable ) {
        val orderedAndCastedCols = newSchema.fields.map(f => {
          val dfFieldAsOption = dfSchema.find(_.name == f.name)
          require( dfFieldAsOption.isDefined)
          val dfField = dfFieldAsOption.get
          if ( dfField.nullable && dfField.dataType.typeName == f.dataType.typeName )
            col(f.name)
          else if ( dfField.nullable && dfField.dataType.typeName != f.dataType.typeName )
            col(f.name).cast(f.dataType).as(f.name)
          else
            when(col(f.name).isNotNull, col(f.name).cast(f.dataType)).otherwise(lit(null).cast(f.dataType)).as(f.name)
        }
        )
        df.select(orderedAndCastedCols: _*).persist(StorageLevel.MEMORY_AND_DISK)
      }
      else
        df.persist(StorageLevel.MEMORY_AND_DISK)
    }

  }

}
