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
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.functions.{col, max, udf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable
import com.dbtimes.dw.common.MiscHelper.getDateFormatted

object DataFrameHelper {

  def readDfFromSqlServer(userAsOption: Option[String],
                        passwordAsOption: Option[String],
                        connectionUriAsOption: Option[String],
                        sourceTableAsOption: Option[String],
                        partitionColumnAsOption: Option[String] = None,
                        numberOfPartitions: Int = 0,
                        dbmsSpecificAttributes: Map[String,String] = Map[String,String](), // not currently used for SQL server
                        resultSchema: StructType                                           // not currently used for SQL server
                       ): DataFrame = {

    readFromJdbcDdms( userAsOption,
      passwordAsOption,
      connectionUriAsOption,
      sourceTableAsOption,
      partitionColumnAsOption,
    numberOfPartitions,
    dbmsSpecificAttributes,
    resultSchema,
      SqlServer )

  }

  def readDfFromOracle(userAsOption: Option[String],
      passwordAsOption: Option[String],
      connectionUriAsOption: Option[String],
      sourceTableAsOption: Option[String],
      partitionColumnAsOption: Option[String] = None,
      numberOfPartitions: Int = 0,
      dbmsSpecificAttributes: Map[String,String] = Map[String,String](), // not currently used for oracle
      resultSchema: StructType                                           // not currently used for oracle
  ): DataFrame = {

    readFromJdbcDdms( userAsOption,
      passwordAsOption,
      connectionUriAsOption,
      sourceTableAsOption,
      partitionColumnAsOption,
      numberOfPartitions,
      dbmsSpecificAttributes,
      resultSchema,
      Oracle )

/*
    val spark = SparkSession.builder().getOrCreate() // this gets previously created session

    Class.forName("oracle.jdbc.driver.OracleDriver") // This call just make sure the class is available later on. It is not otherwise needed

    // Create a Properties() object to hold the parameters.
    val connectionProperties = new Properties()
    val driverClass = "oracle.jdbc.driver.OracleDriver"
    val connectionUri = connectionUriAsOption.get

    connectionProperties.put("Driver", driverClass)
    connectionProperties.put("url", connectionUri )
    connectionProperties.put("user", userAsOption.get)
    connectionProperties.put("password", passwordAsOption.get)
    connectionProperties.put("dbtable", sourceTableAsOption.get)


    val minValue = if (partitionColumnAsOption.isDefined) { // load using partitions
      spark.read.jdbc(
        connectionUri,
        "( SELECT CAST( NVL( MIN( <@PartitionColumn> ), 0 ) AS NUMBER(19,0) ) AS Result FROM <@SourceTable> )"
          .replace("<@PartitionColumn>", partitionColumnAsOption.get)
          .replace("<@SourceTable>", sourceTableAsOption.get),
        connectionProperties).head.getDecimal(0).longValueExact()
    } else 0L

    val maxValue = if (partitionColumnAsOption.isDefined) {
      spark.read.jdbc(
        connectionUri,
        "( SELECT CAST( NVL( MAX( <@PartitionColumn> ), 0 ) AS NUMBER(19,0) ) AS Result FROM <@SourceTable> )"
          .replace("<@PartitionColumn>", partitionColumnAsOption.get)
          .replace("<@SourceTable>", sourceTableAsOption.get),
        connectionProperties).head.getDecimal(0).longValueExact()
    } else 0L

    if (partitionColumnAsOption.isDefined) { // load using partitions
      spark.read.jdbc(
        connectionUri,
        sourceTableAsOption.get,
        partitionColumnAsOption.get,
        minValue,
        maxValue,
        numberOfPartitions,
        connectionProperties)
    }
    else {
      spark.read.jdbc(connectionUri, sourceTableAsOption.get, connectionProperties)
    }
*/
  }

  def readDfFromMongoDb(userAsOption: Option[String],
      passwordAsOption: Option[String],
      connectionUriAsOption: Option[String],
      sourceTableAsOption: Option[String],
      partitionColumnAsOption: Option[String] = None,
      numberOfPartitions: Int = 0,
      dbmsSpecificAttributes: Map[String,String] = Map[String,String](),
      resultSchema: StructType
  ): DataFrame = {
    val spark = SparkSession.builder().getOrCreate() // this gets previously created session

    // if username and password are defined and connection string does not have them, inssert them into connection string
    val connectionUri = if (userAsOption.isDefined && passwordAsOption.isDefined && !connectionUriAsOption.get.contains('@')) {
      val splitOnTwoForwardSlaches = connectionUriAsOption.get.split("//")
      splitOnTwoForwardSlaches(0) + "//" + userAsOption.get + ":" + passwordAsOption.get + "@" + splitOnTwoForwardSlaches(1)
    }
    else
      connectionUriAsOption.get

    val baseReader = spark.read
      .format("mongodb")
      .option("spark.mongodb.read.connection.uri", connectionUri )

    val readerWithCollection = if ( sourceTableAsOption.isDefined ) // For Mongo DB sourceTable will have collection name if defined. If it is not defined the collection name should be part of mongo specific attributes
      baseReader.option("spark.mongodb.read.collection", sourceTableAsOption.get )
    else {
      // the expectation here is that collection is defined within dbmsSpecificAttributes
      baseReader
    }

    val readerWithSchema = if ( !resultSchema.isEmpty ) {
      readerWithCollection.schema( resultSchema )
    }
    else
      readerWithCollection

    readerWithSchema.options(dbmsSpecificAttributes) // Apply aggregation pipeline and other attributes. collection can also be defined here - in that case it will override the value from sourceTable
      .load()
  }

  private def readFromJdbcDdms( userAsOption: Option[String],
      passwordAsOption: Option[String],
      connectionUriAsOption: Option[String],
      sourceTableAsOption: Option[String],
      partitionColumnAsOption: Option[String] = None,
      numberOfPartitions: Int = 0,
      dbmsSpecificAttributes: Map[String,String] = Map[String,String](), // not currently used for SQL server
      resultSchema: StructType,                                           // not currently used for SQL server,
      dbms: DbmsVal ): DataFrame = {

    val spark = SparkSession.builder().getOrCreate() // this gets previously created session

    val driverClass = dbms.driverClass
    Class.forName( driverClass ) // This call just make sure the class is available later one. It is not otherwise needed

    val dbmsAttributes = mutable.Map.from(dbmsSpecificAttributes)
    dbmsAttributes("url") = connectionUriAsOption.get
    dbmsAttributes( "driver" ) = driverClass

    // Create a Properties() object to hold the parameters.
    val connectionProperties = new Properties()
    connectionProperties.setProperty("Driver", driverClass)

    if (userAsOption.isDefined && passwordAsOption.isDefined) {
      connectionProperties.put("user", userAsOption.get)
      connectionProperties.put("password", passwordAsOption.get)
      dbmsAttributes( "user" ) = userAsOption.get
      dbmsAttributes( "password" ) = passwordAsOption.get
    }

    if ( sourceTableAsOption.isDefined )
      dbmsAttributes("dbtable") = sourceTableAsOption.get

    // Action custom validator insures that missing lower or upper bounds can only be for integral data types
    if (partitionColumnAsOption.isDefined) {
      dbmsAttributes("partitionColumn") = partitionColumnAsOption.get
      dbmsAttributes("numPartitions") = numberOfPartitions.toString
    }

    if (partitionColumnAsOption.isDefined && !dbmsAttributes.contains("lowerBound") ) {

      val datasetWithLowerBound = spark.read.jdbc(
        dbmsAttributes("url"),
        dbms.statementForMinLongValue
          .replace("<@PartitionColumn>", partitionColumnAsOption.get)
          .replace("<@SourceTable>", sourceTableAsOption.get ),
        connectionProperties).head

      dbmsAttributes("lowerBound") = if ( dbms.methodsToGetLongValue == "getLong")
        datasetWithLowerBound.getLong(0).toString
      else if ( dbms.methodsToGetLongValue == "getDecimal.longValueExact" )
        datasetWithLowerBound.getLong(0).toString
      else
        throw new RuntimeException("""ERROR: Unknown method to get Long value from min/max dataset.""")
    }

    if (partitionColumnAsOption.isDefined && !dbmsAttributes.contains("upperBound")) {
      val datasetWithUpperBound = spark.read.jdbc(
        dbmsAttributes("url"),
        dbms.statementForMaxLongValue
          .replace("<@PartitionColumn>", partitionColumnAsOption.get)
          .replace("<@SourceTable>", sourceTableAsOption.get ),
        connectionProperties).head

      dbmsAttributes("upperBound") = if ( dbms.methodsToGetLongValue == "getLong")
        datasetWithUpperBound.getLong(0).toString
      else if ( dbms.methodsToGetLongValue == "getDecimal.longValueExact" )
        datasetWithUpperBound.getDecimal(0).longValueExact().toString
      else
        throw new RuntimeException("""ERROR: Unknown method to get Long value from min/max dataset.""")
    }

    spark.read.format( "jdbc" )
      .options( dbmsAttributes )
      .load()
  }

  /***
   *
   * @param calendarStart
   * @param calendarEnd
   * @return data frame with calendar dates in the range from start to end inclusive
   *
   * Sample usage:
   *  override def getCustomDatesToProcess(dfDatesToProcessBasedOnConfig: DataFrame): Option[DataFrame] = {
   *    val start =  new SimpleDateFormat("yyyy-MM-dd").parse( "2019-01-01")
   *    val calendarStart: Calendar = Calendar.getInstance()
   *    calendarStart.setTime(start)
   *    val calendarEnd: Calendar = Calendar.getInstance()  // current date
   *
   *    val dfDatesToProcess = DataFrameHelper.getCalendarDatesDataFrame( calendarStart, calendarEnd)
   *    Some(dfDatesToProcess)
   *  }
   */
  def getCalendarDatesDataFrame( calendarStart: Calendar, calendarEnd: Calendar ): DataFrame = {
    val spark = SparkSession.builder().getOrCreate() // this gets previously created session
    val endDate = getDateFormatted( calendarEnd.getTime(), "yyyy-MM-dd")

    var dates: mutable.Seq[Row] = mutable.Seq.empty[Row]
    dates = dates :+ Row( getDateFormatted( calendarStart.getTime(), "yyyy-MM-dd") )
    do {
      calendarStart.add( Calendar.DATE, 1 )
      val newDate = calendarStart.getTime()
      dates = dates :+ Row( getDateFormatted( newDate, "yyyy-MM-dd") )
      calendarStart.setTime( newDate )
    } while ( endDate != getDateFormatted( calendarStart.getTime(), "yyyy-MM-dd") )

    val schema = StructType( Array( StructField( "Date", StringType, true )))
    val rdd = spark.sparkContext.parallelize( dates.toVector ) // convert  mutable.Seq[Row] to immutable[Seq] according to spark 4.1
    val dfDates = spark.createDataFrame( rdd, schema )
    val datesViewName = "CalendarDates" + java.util.UUID.randomUUID.toString.replace("-", "_")
    dfDates.createOrReplaceTempView( datesViewName )

    /* val sqlCalendarDates = s"""SELECT EXPLODE( SEQUENCE( TO_DATE( '2019-01-01'), TO_DATE( '2019-05-01'), INTERVAL 1 DAY ) """
    * */
    val sqlCalendarDates = s"""SELECT CAST( Date AS DATE ) AS CalendarDate FROM $datesViewName """
    val dfCalendarDates = spark.sql( sqlCalendarDates )
    dfCalendarDates
  }

  val concatColumns = udf((r: Row) => {
    val s = r.mkString("~")
    s
  })

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
      // modify [[StructField] with name `cn`
      val newSchema = StructType(schema.map {
        case StructField(c, t, _, m) => StructField(c, t, nullable = nullable, m)
      })
      // apply new schema
      // df.sqlContext.createDataFrame(df.rdd, newSchema) // old way
      val spark = SparkSession.builder().getOrCreate() // this gets previously created session
      spark.createDataFrame(df.rdd, newSchema)
    }

    def setNewSchema(newSchema: StructType): DataFrame = {
      // apply new schema
      // df.sqlContext.createDataFrame(df.rdd, newSchema) // old way
      val spark = SparkSession.builder().getOrCreate() // this gets previously created session
      spark.createDataFrame(df.rdd, newSchema)
    }

  }


}
