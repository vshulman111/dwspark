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

import com.dbtimes.dw.common.LogFile
import com.typesafe.config.ConfigValue
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigValueType}
import java.text.SimpleDateFormat
import java.util.Calendar

import scala.jdk.CollectionConverters._
import scala.collection.mutable

import LogFile.{logger => comparerLog}
import com.dbtimes.dw.common.DataFrameHelper.concatColumns
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.types.StringType
import org.apache.spark.storage.StorageLevel

import com.dbtimes.dw.common._

object DataSourceComparer {

  def validateConfig(appConfig: Config): Seq[String] = {

    val schemaBaseFileName = "sourcecomparer-config-schema"
    val pathToVersionField = "sourceCompare.sourceCompareConfigVersion"
    var errors: mutable.Seq[String] = mutable.Seq.empty[String]

    // Validate schema in three steps:
    // Step 1. Validate schema version of configuration file
    // Step 2. Load schema of correct version and validate configuration against that version
    // Step 3. Validate schema in the code for components not supported by generic  schema validation
    //    - unique names of actions

    // Do Steps 1 and 2
    errors ++= MiscHelper.validateConfigVersionAndConfigAgainstThatVersion( appConfig, schemaBaseFileName, pathToVersionField )

    // Step 3. Validate schema in the code for components not supported by generic  schema validation
    //          Step 3 depends on schema being validated in steps 1 and 2 because it assumes that path "sourceCompare.compareScenarios" exists and each scenario has a name
    if ( errors.isEmpty ) {
      val compareScenarioConfigs: List[ConfigValue] = appConfig.getList("sourceCompare.compareScenarios").asScala.toList

      // Check: make sure all scenario names are unique
      val scenarioNames: List[String] = compareScenarioConfigs map { case scenarioConfig: ConfigValue => {
        val scenario = new ScenarioConfig(scenarioConfig);
        scenario.getName;
      }
      }
      val duplicateNames = scenarioNames.groupBy(identity)
        .collect { case (x, ys) if ys.size > 1 => x }
      if (!duplicateNames.isEmpty) {
        errors = errors :+ s"""The following scenario names are duplicates: ${duplicateNames.mkString("'", "', '", "'")}. All scenario names must be unique."""
      }

      // Check: loop through all compare scenarios and perform the validation
      val allCustomErrors = compareScenarioConfigs.map( scenario => new ScenarioConfig(scenario).validate );
      errors ++= allCustomErrors.flatten
    }

    errors.toSeq
  }

  def compare(appConfig: Config): Unit = {
    val errors: Seq[String] = DataSourceComparer.validateConfig(appConfig: Config)
    if (errors.nonEmpty) {
      var errorMessage: String = ""
      errors.foreach(error => errorMessage = errorMessage + s"* ${error}\n")
      throw new RuntimeException("Comparer Configuration ERROR(s)\n" + errorMessage)
    }

    val spark = SparkSession.builder().getOrCreate() // this gets previously created session
    val appName = try {
      spark.sparkContext.appName
    }
    catch {
      case e : Exception => ""
    }

    // Create log file if logFileDir is defined
    if ( appConfig.hasPath("sourceCompare.logFileDir") ) {
      LogFile.init( appName, "SOURCE_COMPARER", None, appConfig.getString("sourceCompare.logFileDir"))
    }

    comparerLog.info("---- Starting application " + appName )

    // loop through all compare scenarios and perform the comparisons
    val compareScenarioConfigs: List[ConfigValue] = appConfig.getList("sourceCompare.compareScenarios").asScala.toList
    compareScenarioConfigs.foreach(processCompareScenario)

    comparerLog.info("---- Completed application " + appName )
  }

  private def processCompareScenario(compareScenarioConfig: ConfigValue): Unit = {
    val scenario = new ScenarioConfig(compareScenarioConfig)
    comparerLog.info("---- Processing scenario: " + scenario.getName)
    if (!scenario.getIsActive) {
      comparerLog.info("Skipping processing - the compare scenario is inactive")
    }
    else { // active compare scenario - process it
      val dfLeft = ScenarioSourceLoaderFactory.getScenarioSourceLoader(scenario, true ).loadSource( )
      val dfRight = ScenarioSourceLoaderFactory.getScenarioSourceLoader(scenario, false ).loadSource( )
      val dfDifferences = compareSources( scenario, dfLeft, dfRight )
      val resultFileName = saveDifferences( scenario, dfDifferences )
      if ( !FileHelper.isDirectoryOrFileExists(resultFileName) || FileHelper.isFileExistsAndEmpty(resultFileName))
        comparerLog.info(s"-- DIFFERENCES NOT FOUND for scenario '${scenario.getName}'")
      else
        comparerLog.info(s"-- DIFFERENCES FOUND for scenario '${scenario.getName}'. The compare result saved to file '$resultFileName'" )
    }
  }

  private def compareSources(scenario: ScenarioConfig, dfLeft: DataFrame, dfRight: DataFrame ): DataFrame = {
      // Add concatenated unique key to each source
    val dfLeftWithUniqueKey = addUniqueColumnToSource(scenario, dfLeft)
    val dfRightWithUniqueKey = addUniqueColumnToSource(scenario, dfRight)

    val leftWithUniqueKeyViewName = "LeftWithUniqueKey_" + java.util.UUID.randomUUID.toString.replace("-", "_")
    val rightWithUniqueKeyViewName = "RightWithUniqueKey_" + java.util.UUID.randomUUID.toString.replace("-", "_")
    dfLeftWithUniqueKey.createOrReplaceTempView( leftWithUniqueKeyViewName )
    dfRightWithUniqueKey.createOrReplaceTempView( rightWithUniqueKeyViewName )

    // Determine keys that are in one source only
    val diffsInLeftOnly = getDiffsForInFirstSourcesOnly( scenario, leftWithUniqueKeyViewName, rightWithUniqueKeyViewName, scenario.getMoniker(true), scenario.getMoniker(false))
    val diffsInRightOnly = getDiffsForInFirstSourcesOnly( scenario, rightWithUniqueKeyViewName, leftWithUniqueKeyViewName, scenario.getMoniker(false), scenario.getMoniker(true))

    // Go through all Not Key columns and compare
    val nonUniqueKeys = scenario.getColumnsToCompare( dfLeft.columns.toList)

    if ( !nonUniqueKeys.isEmpty ) {
      nonUniqueKeys.foreach( nonUniqueKey =>
        comparerLog.info(s"Data Type of $nonUniqueKey is ${dfLeftWithUniqueKey.schema(nonUniqueKey).dataType.typeName}")
      )
      val diffsNotEqualList = for ( nonUniqueKeyColumn <- nonUniqueKeys )
        yield if(List("float","double").contains( dfLeftWithUniqueKey.schema(nonUniqueKeyColumn).dataType.typeName))
          getDiffsForNotEqualFloat( scenario, nonUniqueKeyColumn, leftWithUniqueKeyViewName, rightWithUniqueKeyViewName )
        else
          getDiffsForNotEqualNonFloat( scenario, nonUniqueKeyColumn, leftWithUniqueKeyViewName, rightWithUniqueKeyViewName )

      val diffsNotEqual = diffsNotEqualList.reduceLeft((df1, df2) => df1.union(df2) )

      diffsNotEqual
        .union(diffsInLeftOnly)
        .union(diffsInRightOnly)
    }
    else {
      diffsInLeftOnly
        .union(diffsInRightOnly)
    }
    // "float", "double" are the values for floating type. "timestamp", "integer", "string" are the other types
  }

  /**
   * Add new column which is a concatenation of all unique keys
   *
   * @param scenario
   * @param df
   * @return new df with additional column
   */
  private def addUniqueColumnToSource(scenario: ScenarioConfig, df: DataFrame ): DataFrame = {
    val uniqueKeys = scenario.getUniqueKeyColumns
    df.withColumn("UniqueKeyConcat", concatColumns(struct(uniqueKeys.head, uniqueKeys.tail: _*)).cast(StringType))
  }

  private def getDiffsForInFirstSourcesOnly(scenario: ScenarioConfig, viewNameFirst: String, viewNameSecond: String, monikerFirst: String, monikerSecond: String ): DataFrame = {
    val spark = SparkSession.builder().getOrCreate() // this gets previously created session
    val uniqueKeys = scenario.getUniqueKeyColumns
    val sqlDiffsInFirstSourcesOnly =
      s"""
         |SELECT	
         |	${uniqueKeys.mkString("\n  first.`", "`,\n  first.`", "`")},
         |	CAST(NULL AS STRING)	AS `Column Name`		,
         |	CAST(NULL AS STRING)	AS `${monikerFirst} Value`		,
         |	CAST(NULL AS STRING)	AS `${monikerSecond} Value`	,
         |	CAST( 'In ${monikerFirst} only'	AS STRING )	AS Message
         |FROM $viewNameFirst AS first
         |WHERE NOT EXISTS (SELECT 1
         |    							FROM $viewNameSecond AS second
         |							    WHERE second.UniqueKeyConcat = first.UniqueKeyConcat )
         |${if (scenario.getMaxSameDifferences.isDefined) "LIMIT " + scenario.getMaxSameDifferences.get else "" }
         |    """.stripMargin
    comparerLog.info(s"  Creating data frame with differences for 'In ${monikerFirst} only' in using sql:\n" + sqlDiffsInFirstSourcesOnly )
    val dfDiffsInFirstSourcesOnly = spark.sql(sqlDiffsInFirstSourcesOnly)
    if (scenario.getIsDebugDwLib) {
      dfDiffsInFirstSourcesOnly.show(3)
    }
    dfDiffsInFirstSourcesOnly
  }

  private def getDiffsForNotEqualFloat(scenario: ScenarioConfig, columnName: String, viewNameLeft: String, viewNameRight: String ): DataFrame = {
    val spark = SparkSession.builder().getOrCreate() // this gets previously created session
    val uniqueKeys = scenario.getUniqueKeyColumns
    val sqlDiffsNotEqualFloat =
      s"""
         |SELECT
         |	${uniqueKeys.mkString("\n  leftSrc.`", "`,\n  leftSrc.`", "`")},
         |	'$columnName'	AS `Column Name`		,
         |	IFNULL( FORMAT_NUMBER( leftSrc.`$columnName`, 6 ), 'NULL' )	AS `${scenario.getMoniker(true)} Value`		,
         |	IFNULL( FORMAT_NUMBER( rightSrc.`$columnName`, 6 ), 'NULL' )	AS `${scenario.getMoniker(false)} Value`	,
         |	CAST( 'Not equal'	AS STRING )	AS Message
         |FROM $viewNameLeft AS leftSrc
         |  INNER JOIN $viewNameRight AS rightSrc  ON leftSrc.UniqueKeyConcat = rightSrc.UniqueKeyConcat
         |WHERE ABS( IFNULL( leftSrc.`$columnName`, 0 ) - IFNULL( rightSrc.`$columnName`, 0 ) ) > ${scenario.getFloatCompareThreshold.toString}
         |${if (scenario.getMaxSameDifferences.isDefined) "LIMIT " + scenario.getMaxSameDifferences.get else ""}
         |    """.stripMargin
    comparerLog.info(s"  Creating data frame with differences for floating point column `$columnName` using sql:\n" + sqlDiffsNotEqualFloat )
    val dfDiffsNotEqualFloat = spark.sql(sqlDiffsNotEqualFloat)
    if (scenario.getIsDebugDwLib) {
      dfDiffsNotEqualFloat.show(3)
    }
    dfDiffsNotEqualFloat
  }

  private def getDiffsForNotEqualNonFloat(scenario: ScenarioConfig, columnName: String, viewNameLeft: String, viewNameRight: String ): DataFrame = {
    val spark = SparkSession.builder().getOrCreate() // this gets previously created session
    val uniqueKeys = scenario.getUniqueKeyColumns
    val sqlDiffsNotEqualNonFloat =
      s"""
         |SELECT
         |	${uniqueKeys.mkString("\n  leftSrc.`", "`,\n  leftSrc.`", "`")},
         |	'$columnName'	AS `Column Name`		,
         |	IFNULL( CAST( leftSrc.`$columnName` AS STRING), 'NULL' )	AS `${scenario.getMoniker(true)} Value`		,
         |	IFNULL( CAST( rightSrc.`$columnName` AS STRING), 'NULL' )	AS `${scenario.getMoniker(false)} Value`	,
         |	CAST( 'Not equal'	AS STRING )	AS Message
         |FROM $viewNameLeft AS leftSrc
         |  INNER JOIN $viewNameRight AS rightSrc  ON leftSrc.UniqueKeyConcat = rightSrc.UniqueKeyConcat
         |WHERE     ( leftSrc.`$columnName` IS NULL AND rightSrc.`$columnName` IS NOT NULL )
         |      OR  ( leftSrc.`$columnName` IS NOT NULL AND rightSrc.`$columnName` IS NULL )
         |      OR  ( leftSrc.`$columnName` IS NOT NULL AND rightSrc.`$columnName` IS NOT NULL
         |            AND leftSrc.`$columnName` != rightSrc.`$columnName` )
         |${if (scenario.getMaxSameDifferences.isDefined) "LIMIT " + scenario.getMaxSameDifferences.get else ""}
         |            """.stripMargin
    comparerLog.info(s"  Creating data frame with differences for non-floating point column `$columnName` using sql:\n" + sqlDiffsNotEqualNonFloat )
    val dfDiffsNotEqualNonFloat = spark.sql(sqlDiffsNotEqualNonFloat)
    if (scenario.getIsDebugDwLib) {
      dfDiffsNotEqualNonFloat.show(3)
    }
    dfDiffsNotEqualNonFloat
  }

  private def saveDifferences(scenario: ScenarioConfig, dfDifferences: DataFrame ): String = {
    // Create .csv file with differences
    val resultFileBase = FileHelper.makePath(scenario.getCompareResultDir,
      FileHelper.sanitizeFileName( "compare_result_for_"
        + scenario.getName
        + " _"
        + new SimpleDateFormat("yyyyMMdd_hhmmss").format(Calendar.getInstance().getTime()) ) )

    val resultFileName = resultFileBase + ".csv"
    val resultFileDataDir = resultFileBase + "_data"
//    val resultFileHeaderDir = resultFileBase + "_header"  // when merging create header in a separate directory and merge with individual .csv files without  header records

    dfDifferences.persist(StorageLevel.MEMORY_AND_DISK)

    dfDifferences
      .coalesce(1)  // so there is only one file is created
      .write.format("com.databricks.spark.csv")
      .option("header", true )
      .csv( resultFileDataDir )

      // Concat is not supported on all OS, so cannot use it for now
//    FileHelper.concatCsvFiles( resultFileName, List( resultFileDataDir))
    FileHelper.moveFirstCsvFileInDirectory( resultFileName, resultFileDataDir)
    resultFileName
  }

}
