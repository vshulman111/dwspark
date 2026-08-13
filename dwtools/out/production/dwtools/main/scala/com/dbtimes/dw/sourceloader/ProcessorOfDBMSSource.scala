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
package com.dbtimes.dw.sourceloader

import com.dbtimes.dw.common._
import org.apache.spark.sql.SparkSession
import java.util.{Calendar, Date}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import LogFile.{logger => loaderLog}

private[sourceloader] class ProcessorOfDBMSSource(private val action: LoadAction ) extends ActionProcessor
  with SourceDataMerger {

  private[sourceloader] def process(): Unit = {
    val spark = SparkSession.builder().getOrCreate() // this gets previously created session

    val timeStart = Calendar.getInstance().getTimeInMillis()

    // determine method to read from specific DB
    val readFromDBMSSource: ( Option[String],Option[String],Option[String], Option[String], Option[String],Int, Map[String,String], StructType ) => DataFrame
    = if (action.getIsSqlServerSource) DataFrameHelper.readDfFromSqlServer
    else if (action.getIsOracleSource) DataFrameHelper.readDfFromOracle
    else if (action.getIsMongoDbSource) DataFrameHelper.readDfFromMongoDb
    else throw new RuntimeException("""ERROR: Cannot determine method to read from DBMS - unsupported DBMS""")

    // Get data for incremental load if incremental load attributes are available
    val isDoIncrementalLoad = action.isIncrementalLoad

    try {
      val sourceTableChangesSqlAsOption = if (isDoIncrementalLoad) {

        val columnNameAndPlaceholderPairs = action.getIncrementalLoadColumnNameAndPlaceholderPairs

        // 1. first get MAX values for each column name from the existing file
        // The value is string representation of the column's values, i.e. it is cast to String
        val placeholderAndMaxValues = for ((colName, placeholder) <- columnNameAndPlaceholderPairs;
                                           dfMaxNativeColumnType = spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
                                             .load(action.getSdaDestinationFilePath)
                                             .agg(max(colName));
                                           dfMaxAsString = dfMaxNativeColumnType.withColumn(dfMaxNativeColumnType.schema.fieldNames(0), col(dfMaxNativeColumnType.schema.fieldNames(0)).cast(StringType));
                                           maxValueAsString = dfMaxAsString.head.getString(0)
                                           ) yield (placeholder, colName, maxValueAsString)

        // 2. replace all placeholders with max values
        val sourceTableChangesSql = placeholderAndMaxValues.foldLeft(action.getIncrementalLoadSourceTableChangesSql) {
          case (currentSql, (placeholder, colName, null)) =>  throw new RuntimeException(s"""Loader Configuration or data ERROR: MAX value is NULL for column $colName used for incremental load when reading previous version of the file ${action.getSdaDestinationFilePath} """)
          case (currentSql, (placeholder, colName, maxValue)) => currentSql.replaceAll(placeholder, maxValue)
        }

        loaderLog.info("-- Executing sql to get table changes: " + sourceTableChangesSql)

        Some(sourceTableChangesSql)
      } else None

      val dfNewData = readFromDBMSSource(
        action.getUser,
        action.getPassword,
        action.getConnectionUri,
        if (isDoIncrementalLoad) sourceTableChangesSqlAsOption else action.getSourceTable,
        action.getPartitionColumn,
        action.getNumberOfPartitions,
        action.getDbmsSpecificAttributes,
        action.getSchema )


      if (action.getSdaIsDebugDwLib) {
        dfNewData.show(10)
        dfNewData.printSchema()
      }

      //    loaderLog.info("** Processing file " + sourceFilePath )
      val effectiveDate = action.getEffectiveDate(dfNewData)

      // Read merge keys. Need this to handle deleted records
      if (isDoIncrementalLoad) {
        val dfAllMergeKeys = if (action.getPartitionColumn.isDefined && action.getSdaMergeKeysList.contains(action.getPartitionColumn.get)) {
          readFromDBMSSource(
            action.getUser,
            action.getPassword,
            action.getConnectionUri,
            Some(buildQueryToGetAllUniqueKeys(action)),
            action.getPartitionColumn,
            action.getNumberOfPartitions,
            action.getDbmsSpecificAttributes,
            StructType(Seq()) )    // do not use schema for incremental load since it used only for oracle and SQL server and for them, we do not use schema
        }
        else {
          // Do not use partition column, even if defined, because it is not part of merge keys
          readFromDBMSSource(
            action.getUser,
            action.getPassword,
            action.getConnectionUri,
            Some(buildQueryToGetAllUniqueKeys(action)),
            None,
            0,
            action.getDbmsSpecificAttributes,
            StructType(Seq()) )   // do not use schema for incremental load since it used only for oracle and SQL server and for them, we do not use schema
        }

        if (action.getSdaIsDebugDwLib) {
          dfAllMergeKeys.show(10)
          dfAllMergeKeys.printSchema()
        }

        mergeSourceDataChanges(action, dfNewData, isNewDataPreparedForMerge = false, dfAllMergeKeys, effectiveDate, loaderLog)
      } else {
        mergeSourceData(action, dfNewData, effectiveDate, loaderLog)
      }
      createLoadControlRecordFromFileDestinationParquet(action, timeStart, effectiveDate, isSuccess = true, "")
    }
    catch {
      case e: Exception => {
        createLoadControlRecordFromFileDestinationParquet(action, timeStart, effectiveDate = null, isSuccess = false, e.getMessage)
        throw new Exception(e)
      }
    }
  }

  private[sourceloader] def buildQueryToGetAllUniqueKeys(action: LoadAction): String = {
    val query = if (action.getIsSqlServerSource || action.getIsOracleSource) {
      // For sqlServer or oracle it is required to have uniqueKeys table defined in incrementalLoad
      // the uniqueKey value can be a SELECT expression or just a table name
      // If it is a select expression - take it as is. Otherwise build a quesry
      val uniqueKeysSqlOrTable = action.getIncrementalLoadUniqueKeysSql.get
      if ( uniqueKeysSqlOrTable.toUpperCase().contains( "SELECT" ) )
        uniqueKeysSqlOrTable
      else {
        if (action.getIsSqlServerSource ) {
          // The query will look like this: ( SELECT list-of-unique-keys-bracketed FROM source-table ) AS table-alias
          s"""( SELECT ${action.getSdaPrimaryKeysList.mkString("[", "], [", "]")}  FROM ${uniqueKeysSqlOrTable} ) AS tbl${java.util.UUID.randomUUID.toString.replace("-", "")} """
        }
        else if (action.getIsOracleSource)

          // The Oracle query will look like this: ( SELECT list-of-unique-keys FROM source-table )
          s"""( SELECT ${action.getSdaPrimaryKeysList.mkString(",")}  FROM ${uniqueKeysSqlOrTable} ) """
        else
          throw new RuntimeException("""ERROR: Cannot build Query To Get All Unique Keys - unsupported DBMS""")
      }
    }
    else if (action.getIsMongoDbSource) {
      // For mongo DB uniqueKeys is optional. If it does not exist create an aggregation with uniquee keys
      if ( action.getIncrementalLoadUniqueKeysSql.isDefined )
        action.getIncrementalLoadUniqueKeysSql.get
      else {
        /*
          The resulting aggregation would look like this
         [
          {
            $project:
               {
                _id: 0,
                "key1": { $ifNull: [ "$key1", null ] },
                "key2": { $ifNull: [ "$key2", null ] }
              }
          }
        ]
       */

        s"""[ { $$project : { _id: 0, ${action.getSdaPrimaryKeysList.map(s => s""" "$s": { $$ifNull: [ "$s", null ] } """).mkString( "," ) }  """
      }
    }
    else
      throw new RuntimeException("""ERROR: Cannot build Query To Get All Unique Keys - unsupported DBMS""")

    query
  }

}
