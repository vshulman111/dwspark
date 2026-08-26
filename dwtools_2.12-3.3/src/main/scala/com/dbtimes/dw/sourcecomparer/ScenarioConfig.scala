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
package com.dbtimes.dw.sourcecomparer

import java.text.SimpleDateFormat
import java.text.DateFormat
import java.util.Date
import com.typesafe.config.{Config, ConfigValue}
import scala.collection.JavaConverters._
// import scala.jdk.CollectionConverters._ // Scala 2.13 // is needed for .asScala method when getting a list
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.types.StructType
import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.dbtimes.dw.common._

import scala.collection.mutable

private[sourcecomparer] class ScenarioConfig(val scenarioConfig: ConfigValue) extends ConfigDwJobsCommon {

  private val scenario: Config = scenarioConfig.atKey("scenario") // giving this object a key allows reference this element as root by name

  /**
   * Validate scenario against expected structure
   */
  private[sourcecomparer] def validate: Seq[String] = {
    var errors: mutable.Seq[String] = mutable.Seq.empty[String]

      // Check: scenario must have different left and right monikers
    if (getMoniker(true) == getMoniker(false))
      errors = errors :+ s"""Scenario "$getName" has the same moniker for left and right sources. The monikers must be different."""

    errors.toSeq
  }

  // Methods applicable to all types of sources to be compared
  private[sourcecomparer] def getIsActive: Boolean = scenario.getBoolean("scenario.isActive")

  private[sourcecomparer] def getIsDebugDwLib: Boolean = if (!scenario.hasPath("scenario.isDebugDwLib")) false else scenario.getBoolean("scenario.isDebugDwLib") // no debug by default

  private[sourcecomparer] def getName: String = scenario.getString("scenario.name")

  private[sourcecomparer] def getFloatThreshold: Double = {
    if (scenario.hasPath("scenario.floatThreshold") ) {
      scenario.getDouble("scenario.floatThreshold")
    }
    else {
      0 // Default is no threshold
    }
  }

  private[sourcecomparer] def getTimestampTruncateUnit: Option[String] = {
    if (scenario.hasPath("scenario.timestampTruncateUnit") ) {
      Some( scenario.getString("scenario.timestampTruncateUnit"))
    }
    else {
      None
    }
  }

  private[sourcecomparer] def getDateTruncateUnit: Option[String] = {
    if (scenario.hasPath("scenario.dateTruncateUnit") ) {
      Some( scenario.getString("scenario.dateTruncateUnit"))
    }
    else {
      None
    }
  }

  private[sourcecomparer] def getMaxSameDifferences: Option[Int] = {
    if (scenario.hasPath("scenario.maxSameDifferences") ) {
      Some( scenario.getInt("scenario.maxSameDifferences") )
    }
    else {
      None // Default is return all differences
    }
  }

  private[sourcecomparer] def getCompareResultDir: String =
    if ( scenario.hasPath("scenario.compareResult.file.dir"))
      scenario.getString("scenario.compareResult.file.dir")
    else
      throw new RuntimeException(s"""Comparer Configuration ERROR: scenario does not have required attribute "compareResult.dir" attribute defined """)


  /**
   * This  is the schema for comparison result set.
   * By default, the unique key is included. If isInclude is set to false for unique key,it will be excluded
   * @return
   */
  // private[sourcecomparer] def getSchema: StructType = super.getSchema(scenario, "scenario.compareResult.schema")

  private[sourcecomparer] def getUniqueKeyColumns: List[String] = {
    val uniqueKeyColumns = super.getSchemaColNamesWithFlag(
      scenario, "scenario.compareResult.schema", "isUniqueKey", true, Some(false))
    uniqueKeyColumns.diff(getColumnsToExcludeFromComparison)
  }

  /**
   * One or more columns can be excluded from comparison. All other columns will be included.
   * @return
   */
  private def getColumnsToExcludeFromComparison: List[String] = super.getSchemaColNamesWithFlag(
      scenario, "scenario.compareResult.schema", "isInclude", false, None)

  /**
   * By default, all columns are included in comparison, unless at least one column has explicit isInclude flag set to true.
   * If that's the case only columns with isInclude flag set will be included into comparison.
   * If columns are listed that do not have isInclude flag, they will be ignored, unless they are keys.
   * For keys the default for isInclude is true .
   * @return
   */
  private  def getColumnsToIncludeInComparison: List[String] = super.getSchemaColNamesWithFlag(
    scenario, "scenario.compareResult.schema", "isInclude", true, None)

  /**
   *
   * @param allColumns - includes all columns in the result set. This list does not include concatenated unique key.
   * @return - the list of columns to compare. This is the
   *         1) list of all columns that are explicitly included into comparison minus primary keys columns, or
   *         2) list of all columns excluding primary key columns and columns to be excluded from comparison based on configuration
   */
  private[sourcecomparer]   def getColumnsToCompare( allColumns: List[String] ): List[String] = {
    val uniqueKeys = getUniqueKeyColumns
    val excludeList = getColumnsToExcludeFromComparison
    val includeList = getColumnsToIncludeInComparison

    if ( !includeList.isEmpty ) {
      allColumns.intersect( includeList ).diff( uniqueKeys )
    }
    else {
      allColumns.diff(uniqueKeys).diff(excludeList)
    }
  }


  // Methods applicable to source
  private def getLeftOrRight( isLeftSource: Boolean ): String = if (isLeftSource ) "left" else "right"

  private[sourcecomparer] def getIsFileSource(isLeftSource: Boolean): Boolean = if (scenario.hasPath(s"scenario.${getLeftOrRight(isLeftSource)}Source.file")) true else false
  private[sourcecomparer] def getIsFileSourceCsv(isLeftSource: Boolean): Boolean = if (scenario.hasPath(s"scenario.${getLeftOrRight(isLeftSource)}Source.file.csv")) true else false
  private[sourcecomparer] def getIsFileSourceParquet(isLeftSource: Boolean): Boolean = if (scenario.hasPath(s"scenario.${getLeftOrRight(isLeftSource)}Source.file.parquet")) true else false
  private[sourcecomparer] def getIsDbmsSource(isLeftSource: Boolean): Boolean = if (scenario.hasPath(s"scenario.${getLeftOrRight(isLeftSource)}Source.dbms")) true else false
  private[sourcecomparer] def getIsDbmsSourceSqlServer(isLeftSource: Boolean): Boolean = if (scenario.hasPath(s"scenario.${getLeftOrRight(isLeftSource)}Source.dbms.sqlServer")) true else false
  private[sourcecomparer] def getIsDbmsSourceOracle(isLeftSource: Boolean): Boolean = if (scenario.hasPath(s"scenario.${getLeftOrRight(isLeftSource)}Source.dbms.oracle")) true else false
  private[sourcecomparer] def getIsDbmsSourceMongoDb(isLeftSource: Boolean): Boolean = if (scenario.hasPath(s"scenario.${getLeftOrRight(isLeftSource)}Source.dbms.mongoDb")) true else false

  private[sourcecomparer] def getSchema(isLeftSource: Boolean): StructType =
    super.getSchema(scenario,s"scenario.${getLeftOrRight(isLeftSource)}Source.schema" )

  private[sourcecomparer] def getMoniker(isLeftSource: Boolean): String = {
    if ( scenario.hasPath(s"scenario.${getLeftOrRight(isLeftSource)}Source.moniker"))
      scenario.getString(s"scenario.${getLeftOrRight(isLeftSource)}Source.moniker")
    else
      getLeftOrRight(isLeftSource)  // default left or right
  }

  private[sourcecomparer] def getSubsetQueryToCreateSubsetToCompare( isLeftSource: Boolean): Option[String] = {
    if ( scenario.hasPath(s"scenario.${getLeftOrRight(isLeftSource)}Source.subsetQuery")) {
      val subsetQuery: ConfigValue = scenario.getValue( s"scenario.${getLeftOrRight(isLeftSource)}Source.subsetQuery" )
      if ( subsetQuery.unwrapped().getClass.toString == "class java.util.ArrayList" ) {
        Some( subsetQuery.unwrapped().asInstanceOf[java.util.ArrayList[String]].asScala.mkString( "\n" ) )  // Convert array to string
      }
      else // string
        Some(scenario.getString(s"scenario.${getLeftOrRight(isLeftSource)}Source.subsetQuery"))
    } else
      None
  }

  // File related methods
  private[sourcecomparer] def getSourceFilePath(isLeftSource: Boolean): String = {
    if ( getIsFileSource( isLeftSource ) ) {
      scenario.getString(s"scenario.${getLeftOrRight(isLeftSource)}Source.file.path")
    } else {
      throw new RuntimeException(s"""Comparer Configuration ERROR: file source must have "path" defined""")
    }
  }

  //
  // Methods applicable to file source - csv
  // -----------------------------------------
  private[sourcecomparer] def getCsvOptions(isLeftSource: Boolean): Map[String,String] = {
    super.getConfigObjectFieldsAndValues(if ( getIsFileSourceCsv(isLeftSource) ) Some( scenario.getConfig(s"scenario.${getLeftOrRight(isLeftSource)}Source.file.csv") ) else None )
  }

  //
  // Methods applicable to dbms source
  // -----------------------------------
  private[sourcecomparer] def getConnectionUri( isLeftSource: Boolean): Option[String] = {
    if (getIsDbmsSource( isLeftSource ) ) {
      val dbmsAttributes: Map[String,String] = getDbmsSpecificAttributes( isLeftSource )

      if ( ( getIsDbmsSourceSqlServer(isLeftSource) || getIsDbmsSourceOracle(isLeftSource) ) && dbmsAttributes.contains("url") ) {
        dbmsAttributes.get("url")
      }
      else if ( getIsDbmsSourceMongoDb(isLeftSource) && dbmsAttributes.contains("connection.uri") ) {
        dbmsAttributes.get("connection.uri")
      }
      else {
        None
      }
    }
    else
      None
  }

  private[sourcecomparer] def getUser( isLeftSource: Boolean): Option[String] = {
    if ( getIsDbmsSource( isLeftSource ) && scenario.hasPath(s"scenario.${getLeftOrRight(isLeftSource)}Source.dbms.user"))
      Some(scenario.getString(s"scenario.${getLeftOrRight(isLeftSource)}Source.dbms.user"))
    else
      None
  }

  private[sourcecomparer] def getPassword( isLeftSource: Boolean): Option[String] = {
    if ( getIsDbmsSource( isLeftSource ) && scenario.hasPath(s"scenario.${getLeftOrRight(isLeftSource)}Source.dbms.password"))
      Some(scenario.getString(s"scenario.${getLeftOrRight(isLeftSource)}Source.dbms.password"))
    else
      None
  }

  private[sourcecomparer] def getSourceTable( isLeftSource: Boolean): Option[String] = {
    if (getIsDbmsSource( isLeftSource ) ) {
      val dbmsAttributes: Map[String,String] = getDbmsSpecificAttributes( isLeftSource )

      if ( ( getIsDbmsSourceSqlServer(isLeftSource) || getIsDbmsSourceOracle(isLeftSource) ) && dbmsAttributes.contains("dbtable") ) {
        dbmsAttributes.get("dbtable")
      }
      else if ( getIsDbmsSourceMongoDb(isLeftSource) && dbmsAttributes.contains("aggregation.pipeline") ) {
        dbmsAttributes.get("aggregation.pipeline")
      }
      else {
        None
      }
    }
    else
      None
  }

  /**
   * We require to define the partition column in the schema so we can calculate min and max values for INT type of the column
   * if the boundaries are not defined. For that we need to know the type of the partitioning column that is defined in the schema only
   * For that reason we disallow defining partitionColumn in the database object. That is enforced by schema validation.
   * @param isLeftSource
   * @return
   */
  private[sourcecomparer] def getPartitionColumn( isLeftSource: Boolean): Option[String] = {
    // schema is not required and may be absent if there is no partitioning column for Spark partitioning read
    if ( getIsDbmsSource( isLeftSource ) && scenario.hasPath(s"scenario.${getLeftOrRight(isLeftSource)}Source.schema")) {
      val partitionColumns = super.getSchemaColNamesWithFlag(
      scenario, s"scenario.${getLeftOrRight(isLeftSource)}Source.schema", "isPartitionColumn", true, Some(false))
      partitionColumns.headOption
    } else {
      None
    }
  }

  private[sourcecomparer] def getNumberOfPartitions( isLeftSource: Boolean): Int = {
    if (getIsDbmsSource( isLeftSource ) ) {
      val dbmsAttributes: Map[String,String] = getDbmsSpecificAttributes( isLeftSource )

      if ( ( getIsDbmsSourceSqlServer(isLeftSource) || getIsDbmsSourceOracle(isLeftSource) ) && dbmsAttributes.contains("numPartitions") ) {
        dbmsAttributes("numPartitions").toInt
      }
      else {
        1 // default number of partitions
      }
    }
    else
      1 // default number of partitions
  }

  private[sourcecomparer] def getDbmsSpecificAttributes( isLeftSource: Boolean): Map[String,String] = {
    if ( getIsDbmsSourceSqlServer(isLeftSource) )
      super.getConfigObjectFieldsAndValues( Some( scenario.getConfig(s"scenario.${getLeftOrRight(isLeftSource)}Source.dbms.sqlServer") ) )
    else if ( getIsDbmsSourceOracle(isLeftSource) )
      super.getConfigObjectFieldsAndValues( Some( scenario.getConfig(s"scenario.${getLeftOrRight(isLeftSource)}Source.dbms.oracle") ) )
    else if ( getIsDbmsSourceMongoDb(isLeftSource) )
      super.getConfigObjectFieldsAndValues( Some( scenario.getConfig(s"scenario.${getLeftOrRight(isLeftSource)}Source.dbms.mongoDb") ) )
    else
      Map[String,String] ()
  }
}
