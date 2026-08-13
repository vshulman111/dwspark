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

// import scala.collection.mutable.Map

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.dbtimes.dw.common.DataFrameHelper.{DataFrameImplicits, concatColumns}
import com.dbtimes.dw.common.MiscHelper.getDateFormatted
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

private[dw] trait SourceDataMerger {

  // columns added to the data source. The names can be prefixed with arbitrary string to avoid possible
  // collision with actual data source columns which can have the same column name.
  private val serviceColsBaseNames: Map[String, String] = Map(
    ("RowUniqueKey" -> "RowUniqueKey"),
    ("RowMergeKey" -> "RowMergeKey"),
    ("RowHash" -> "RowHash"),
    ("RowTimestamp" -> "RowTimestamp"),
    ("CreatedOn" -> "CreatedOn"),
    ("CreatedBy" -> "CreatedBy"),
    ("Version" -> "Version"),
    ("EffectiveDate" -> "EffectiveDate"),
    ("EffectiveDateStart" -> "EffectiveDateStart"),
    ("EffectiveDateEnd" -> "EffectiveDateEnd"))
  private var serviceCols: Map[String, String] = Map.empty

  private val farFutureDateYYYY_MM_DD = "2099-01-01"

  protected def mergeSourceData(action: SourceDataAction, dfNewData: DataFrame, effectiveDate: Date, appLog: Logger): DataFrame = {
    mergeSourceData(action, true, dfNewData, false, None, None, effectiveDate, appLog)
  }

  protected def mergeSourceDataChanges(action: SourceDataAction, dfNewData: DataFrame, isNewDataPreparedForMerge: Boolean, dfAllMergeKeys: DataFrame, dfOldData: DataFrame, effectiveDate: Date, appLog: Logger): DataFrame = {
    mergeSourceData(action, false, dfNewData, isNewDataPreparedForMerge, Some(dfAllMergeKeys), Some(dfOldData), effectiveDate, appLog)
  }

  protected def mergeSourceDataChanges(action: SourceDataAction, dfNewData: DataFrame, isNewDataPreparedForMerge: Boolean, dfAllMergeKeys: DataFrame, effectiveDate: Date, appLog: Logger): DataFrame = {
    mergeSourceData(action, false, dfNewData, isNewDataPreparedForMerge, Some(dfAllMergeKeys), dfOldDataAsOption = None, effectiveDate, appLog)
  }

  /**
   *
   * @param action
   * @param isFullLoad             - it is full vs. incremental. Incremental means that the new data is changes
   *                               only, not the whole set. If it is the whole set  it is full. Incremental load
   *                               cannot be initial. Subsequent load can be full or incremental.
   * @param dfNewData
   * @param dfAllMergeKeysAsOption -- merge keys can be defined without defining unique keys,
   *                               or they can be a separate set of columns, or be the same a unique keys,
   *                               or a subset of unique keys
   * @param dfOldDataAsOption
   * @param effectiveDate
   * @param appLog
   * @return
   */
  private def mergeSourceData(
      action: SourceDataAction,
      isFullLoad: Boolean,
      dfNewData: DataFrame,
      isNewDataPreparedForMerge: Boolean,
      dfAllMergeKeysAsOption: Option[DataFrame],
      dfOldDataAsOption: Option[DataFrame],
      effectiveDate: Date,
      appLog: Logger): DataFrame = {

    serviceCols = serviceColsBaseNames.transform { (key, value) => action.getSdaServiceColumnPrefix + value } // this is deprecated in later version. Use mapValuesInPlace

    val isInitialLoad = action.getSdaIsInitialLoad

    val dfNewDataPrepared = if (isNewDataPreparedForMerge) dfNewData else addServiceColumnsAndPrepareForMerge(action, dfNewData, effectiveDate, appLog)

    val dfAllMergeKeysWithServiceColumnsAsOption = if (dfAllMergeKeysAsOption.isDefined) {
      val mergeKeys = action.getSdaMergeKeysList
      val dfAllMergeKeysWithServiceColumns = dfAllMergeKeysAsOption.get
        // Service columns
        // add composite unique key. The Dataset must have at least one primary key.
        .withColumn(serviceCols("RowMergeKey"), if (mergeKeys.isEmpty) lit(null).cast(StringType) else concatColumns(struct(mergeKeys.head, mergeKeys.tail: _*)))

      Some(dfAllMergeKeysWithServiceColumns)
    }
    else {
      None
    }

    val sourceHasPrimaryKey = !action.getSdaPrimaryKeysList.isEmpty
    val sourceHasMergeKey = !action.getSdaMergeKeysList.isEmpty

    val dfResult = if (sourceHasPrimaryKey && (isFullLoad || !isFullLoad && action.getSdaMergeKeysList == action.getSdaPrimaryKeysList)) {
      mergeSourceWithUniqueKey(action, isInitialLoad, isFullLoad, dfNewDataPrepared, dfOldDataAsOption, dfAllMergeKeysWithServiceColumnsAsOption, effectiveDate, appLog)
    }
    else if (sourceHasMergeKey && !isFullLoad && !action.getSdaIsVersioned) { // This case cannot currently happen for source loads because we require versioned data for incremental load
      // incremental load: merge keys are different from unique keys
      require(isInitialLoad == false, """Loader ERROR: Incremental load cannot be done on initial load """)
      require(dfAllMergeKeysAsOption.isDefined == true, """Loader ERROR: Incremental load requires defined dfAllMergeKeysAsOption """)
      require(action.getSdaMergeKeysList.isEmpty == false, """Loader ERROR: Incremental load requires a list of one or more Merge Key  """)
      mergeSourceChanges(action, dfNewDataPrepared, dfAllMergeKeysAsOption.get, dfOldDataAsOption, effectiveDate, appLog)
    }
    else if (sourceHasPrimaryKey || sourceHasMergeKey) {
      throw new RuntimeException("""Loader Configuration ERROR: Unsupported combination of load attributes """)
    }
    else {
      mergeSourceWithNoUniqueKey(action, isInitialLoad, dfNewDataPrepared, dfOldDataAsOption, effectiveDate, appLog)
    }
    dfResult
  }

  protected def addServiceColumnsAndPrepareForMerge(action: SourceDataAction, dfNewData: DataFrame, effectiveDate: Date, appLog: Logger): DataFrame = {

    serviceCols = serviceColsBaseNames.transform { (key, value) => action.getSdaServiceColumnPrefix + value } // this is deprecated in later version. Use mapValuesInPlace

    val effDateYYYY_MM_DD = getDateFormatted(effectiveDate, "yyyy-MM-dd")
    val isInitialLoad = action.getSdaIsInitialLoad
    val primaryKeys = action.getSdaPrimaryKeysList
    val mergeKeys = action.getSdaMergeKeysList
    val columnsToExcludeFromVersioning = action.getSdaExcludeFromVersioningColumnList
    val sourceHasPrimaryKey = !action.getSdaPrimaryKeysList.isEmpty
    val sourceHasMergeKey = !action.getSdaMergeKeysList.isEmpty
    // Exclude from RowHash columns with attribute "isExcludeFromVersioning" : "true"
    val columnsForRowHash = dfNewData.columns.toList.diff(columnsToExcludeFromVersioning)


    val dfNewDataPrepared = if (sourceHasPrimaryKey || sourceHasMergeKey) {
      val dfNewDataWithServiceColumnsExceptVersion = dfNewData
        // Service columns
        // add composite unique key. The Dataset must have at least one primary key.
        .withColumn(serviceCols("RowUniqueKey"), if (primaryKeys.isEmpty) lit(null).cast(StringType) else concatColumns(struct(primaryKeys.head, primaryKeys.tail: _*)))
        .withColumn(serviceCols("RowMergeKey"), if (mergeKeys.isEmpty) lit(null).cast(StringType) else concatColumns(struct(mergeKeys.head, mergeKeys.tail: _*)))
        // Before there was expression struct("*") which unintentionally included two just added service columns
        // "RowUniqueKey" and "RowMergeKey". Those columns were not causing an issue but they are not needed in RowHash.
        .withColumn(serviceCols("RowHash"), md5(concatColumns(struct(columnsForRowHash.head, columnsForRowHash.tail: _*))))
        .withColumn(serviceCols("RowTimestamp"), current_timestamp()) // in DB this is ROWVERSION. Here we assume that there is some time between loads that will allow to use timestamp to determine what changed since last load. !! important: we need to maintan this for any changes to the row, like expiring a row
        .withColumn(serviceCols("CreatedOn"), current_timestamp())
        .withColumn(serviceCols("CreatedBy"), lit(System.getProperty("user.name"))) // identity of who runs the job

      if (action.getSdaIsDebugDwLib) {
        appLog.info("All rows count: " + dfNewDataWithServiceColumnsExceptVersion.count())
      }

      // remove duplicates
      val dfNewNoDups = if (sourceHasPrimaryKey && action.getSdaIsRemoveDuplicateRows) {
        dfNewDataWithServiceColumnsExceptVersion.dropDuplicates(serviceCols("RowUniqueKey"))
      }
      else
        dfNewDataWithServiceColumnsExceptVersion

      if (action.getSdaIsDebugDwLib) {
        appLog.info("Distinct count: " + dfNewNoDups.count())
      }

      // Only add version on initial load
      val dfStgNew = if (isInitialLoad && sourceHasPrimaryKey && action.getSdaIsVersioned) {
        appLog.info("Running initial load for versioned data source with unique keys")
        // Only add these service columns on initial load because the data will be saved after that without additional transformations
        // If this is subsequent load the same service columns will be added during processing
        dfNewNoDups
          .withColumn(serviceCols("Version"), lit(1)) // set version to 1 for new data
          .withColumn(serviceCols("EffectiveDateStart"), to_date(lit(effDateYYYY_MM_DD)))
          .withColumn(serviceCols("EffectiveDateEnd"), to_date(lit(farFutureDateYYYY_MM_DD))) // This column must be the last one. It will be replaced during versioning process
      } else {
        dfNewNoDups
      }
      dfStgNew
    }
    else { // source has neither primary or merge keys
      val dfNewDataWithServiceColumns = dfNewData
        // Service columns
        .withColumn(serviceCols("RowTimestamp"), current_timestamp()) // in DB this is ROWVERSION. Here we assume that there is some time between loads that will allow to use timestamp to determine what changed since last load. !! important: we need to maintan this for any changes to the row, like expiring a row
        .withColumn(serviceCols("CreatedOn"), current_timestamp())
        .withColumn(serviceCols("CreatedBy"), lit(System.getProperty("user.name"))) // identity of who runs the job
        .withColumn(serviceCols("EffectiveDate"), to_date(lit(effDateYYYY_MM_DD)))

      if (action.getSdaIsDebugDwLib) {
        appLog.info("All rows count: " + dfNewDataWithServiceColumns.count())
      }
      dfNewDataWithServiceColumns
    }

    dfNewDataPrepared
  }

  protected def createLoadControlRecordFromFileDestinationParquet(
      action: SourceDataAction,
      timeStart: Long,
      effectiveDate: Date,
      isSuccess: Boolean,
      errorMessage: String ): Unit = {

    val isInitialLoad = action.getSdaIsInitialLoad
    val sourceHasPrimaryKey = !action.getSdaPrimaryKeysList.isEmpty
    val result =  if( isSuccess ) "success" else "fail"

    if (action.getSdaIsMaintainLoadControl) {
      val spark = SparkSession.builder().getOrCreate() // this gets previously created session

      val loadDurationSec = (Calendar.getInstance().getTimeInMillis() - timeStart).toDouble / 1e3 // seconds

      val sourceDescriptionAbbreviated  = if ( action.getSdaSourceDescription.length() > 80 ) {
        action.getSdaSourceDescription.substring(0,37)
          .concat("...")
          .concat( action.getSdaSourceDescription.substring(action.getSdaSourceDescription.length() - 39) )  // last 39 characters
      }
      else {
        action.getSdaSourceDescription
      }


      val loadDescription =
        if (isInitialLoad && sourceHasPrimaryKey && action.getSdaIsVersioned) "Initial load of source with unique key to versioned destination"
        else if (isInitialLoad && sourceHasPrimaryKey && !action.getSdaIsVersioned) "Initial load of source with unique key to non-versioned destination"
        else if (isInitialLoad && !sourceHasPrimaryKey && action.getSdaIsVersioned) "Initial load of source with no unique key to versioned destination"
        else if (isInitialLoad && !sourceHasPrimaryKey && !action.getSdaIsVersioned) "Initial load of source with no unique key to non-versioned destination"
        else if (!isInitialLoad && sourceHasPrimaryKey && action.getSdaIsVersioned) "Subsequent load of source with unique key to versioned destination"
        else if (!isInitialLoad && sourceHasPrimaryKey && !action.getSdaIsVersioned) "Subsequent load of source with unique key to non-versioned destination"
        else if (!isInitialLoad && !sourceHasPrimaryKey && action.getSdaIsVersioned) "Subsequent load of source with no unique key to versioned destination"
        else if (!isInitialLoad && !sourceHasPrimaryKey && !action.getSdaIsVersioned) "Subsequent load of source with no unique key to non-versioned destination"
        else ""

      val loadControlRow = if ( !isSuccess ) {
        Seq(
          Row(
            action.getSdaName, // "LoadName"
            action.getSdaSourceTypeDescription, // "SourceType"
            sourceDescriptionAbbreviated, // "ProcessedSource"
            action.getSdaDestinationTypeDescription, // "DestinationType"
            action.getSdaDestinationDescription, // "Destination"
            loadDescription, // "LoadDescription"
            loadDurationSec, // "LoadDurationSec"
            null, // "MaxRowTimestamp"
            result, // "Result"
            null, // "CountTotalAffectedRecords"
            null, // "CountUnchangedRecords"
            null, // "CountNewAndChangedRecords"
            null, // "CountOlderVersions"
            null, // "CountCurrentUnchangedVersions"
            null, // "CountDeletedVersions"
            null, // "CountFirstVersions"
            null, // "CountNewVersions"
            errorMessage // "ErrorMessage"
          ))
      }
      else {
        val effDateYYYY_MM_DD = getDateFormatted(effectiveDate, "yyyy-MM-dd")
        val prevEffDateYYYY_MM_DD = getDateFormatted(effectiveDate, "yyyy-MM-dd", -1)

        val maxRowTimestamp = spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
            .load(action.getSdaDestinationFilePath)
            .agg(max(serviceCols("RowTimestamp"))).head.getTimestamp(0)

        val countTotalAffectedRecords = {
          if (sourceHasPrimaryKey)
            spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
              .load(action.getSdaDestinationFilePath)
              .count()
          else
            spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
              .load(action.getSdaDestinationFilePath)
              .where(s"""${serviceCols("EffectiveDate")} = "$effDateYYYY_MM_DD" """)
              .count()
        }

        val loadStartTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(timeStart)

        val countUnchangedRecords =
          if (sourceHasPrimaryKey && !action.getSdaIsVersioned)
            spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
              .load(action.getSdaDestinationFilePath)
              .where(s"""${serviceCols("RowTimestamp")} < "$loadStartTime" """)
              .count()
          else
            -1

        val countNewAndChangedRecords =
          if (sourceHasPrimaryKey && !action.getSdaIsVersioned)
            spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
              .load(action.getSdaDestinationFilePath)
              .where(s"""${serviceCols("RowTimestamp")} >= "$loadStartTime" """)
              .count()
          else
            -1

        val countOlderVersions =
          if (sourceHasPrimaryKey && action.getSdaIsVersioned)
            spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
              .load(action.getSdaDestinationFilePath)
              .where(s"""${serviceCols("EffectiveDateEnd")} < "$prevEffDateYYYY_MM_DD" """)
              .count()
          else
            -1

        val countCurrentUnchangedVersions =
          if (sourceHasPrimaryKey && action.getSdaIsVersioned)
            spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
              .load(action.getSdaDestinationFilePath)
              .where(s"""${serviceCols("EffectiveDateStart")} < "$effDateYYYY_MM_DD" AND ${serviceCols("EffectiveDateEnd")} = "$farFutureDateYYYY_MM_DD" """)
              .count()
          else
            -1

        val countDeletedVersions =
          if (sourceHasPrimaryKey && action.getSdaIsVersioned)
            spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
              .load(action.getSdaDestinationFilePath)
              .where(s"""${serviceCols("EffectiveDateStart")} < "$effDateYYYY_MM_DD" AND ${serviceCols("EffectiveDateEnd")} = "$prevEffDateYYYY_MM_DD" """)
              .count()
          else
            -1

        val countFirstVersions =
          if (sourceHasPrimaryKey && action.getSdaIsVersioned)
            spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
              .load(action.getSdaDestinationFilePath)
              .where(s"""${serviceCols("EffectiveDateStart")} = "$effDateYYYY_MM_DD" AND ${serviceCols("EffectiveDateEnd")} = "$farFutureDateYYYY_MM_DD" AND ${serviceCols("Version")} = 1 """)
              .count()
          else
            -1

        val countNewVersions =
          if (sourceHasPrimaryKey && action.getSdaIsVersioned)
            spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
              .load(action.getSdaDestinationFilePath)
              .where(s"""${serviceCols("EffectiveDateStart")} = "$effDateYYYY_MM_DD" AND ${serviceCols("EffectiveDateEnd")} = "$farFutureDateYYYY_MM_DD" AND ${serviceCols("Version")} > 1 """)
              .count()
          else
            -1

        // Create new LoadControl record
        Seq(
          Row(
            action.getSdaName, // "LoadName"
            action.getSdaSourceTypeDescription, // "SourceType"
            sourceDescriptionAbbreviated, // "ProcessedSource"
            action.getSdaDestinationTypeDescription, // "DestinationType"
            action.getSdaDestinationDescription, // "Destination"
            loadDescription, // "LoadDescription"
            loadDurationSec, // "LoadDurationSec"
            maxRowTimestamp, // "MaxRowTimestamp"
            result, // "Result"
            countTotalAffectedRecords, // "CountTotalAffectedRecords"
            countUnchangedRecords, // "CountUnchangedRecords"
            countNewAndChangedRecords, // "CountNewAndChangedRecords"
            countOlderVersions, // "CountOlderVersions"
            countCurrentUnchangedVersions, // "CountCurrentUnchangedVersions"
            countDeletedVersions, // "CountDeletedVersions"
            countFirstVersions, // "CountFirstVersions"
            countNewVersions, // "CountNewVersions"
            errorMessage // "ErrorMessage"
          ))
      }

      val loadControlSchema = new StructType(Array(
        //      new StructField( "CreatedOn", TimestampType, true ),
        //      new StructField( "CreatedBy", StringType, true ),
        //      new StructField( "EffectiveDate", TimestampType, true ),
        new StructField("LoadName", StringType, true),
        new StructField("SourceType", StringType, true),
        new StructField("Source", StringType, true),
        new StructField("DestinationType", StringType, true),
        new StructField("Destination", StringType, true),
        new StructField("LoadDescription", StringType, true),
        new StructField("LoadDurationSec", DoubleType, true),
        new StructField("MaxRowTimestamp", TimestampType, true),
        new StructField("Result", StringType, true),
        new StructField("CountTotalRecords", LongType, true),
        new StructField("CountUnchangedRecords", LongType, true),
        new StructField("CountNewAndChangedRecords", LongType, true),
        new StructField("CountOlderVersions", LongType, true),
        new StructField("CountCurrentUnchangedVersions", LongType, true),
        new StructField("CountDeletedVersions", LongType, true),
        new StructField("CountInitialVersions", LongType, true),
        new StructField("CountChangedVersions", LongType, true),
        new StructField("ErrorMessage", StringType, true)
      ))
      val loadControlRDD = spark.sparkContext.parallelize(loadControlRow)
      val loadControlDf = spark.createDataFrame(loadControlRDD, loadControlSchema)
        .withColumn("CreatedOn", current_timestamp())
        .withColumn("CreatedBy", lit(System.getProperty("user.name"))) // identity of who runs the job
        .withColumn("EffectiveDate", if (effectiveDate == null) to_date(lit(null)) else to_date(lit(getDateFormatted(effectiveDate, "yyyy-MM-dd"))))
        .setNullableStateForAllColumns(true)

      if (action.getSdaIsMaintainLoadControlAsParquetFile) {
        if (action.getSdaIsDebugDwLib) {
          loadControlDf.show()
          loadControlDf.printSchema()
        }

        FileHelper.saveDataFrameAsParquetAndMoveToParentDir(loadControlDf, "loadControl", action.getSdaLoadControlParquetFileDir)
      }
      else {
        throw new RuntimeException("""Loader ERROR: Cannot save Load Control record. Only Parquet format is currently supported. Fix configuration for this action to specify "fileLoadControl.parquet" """)
      }
    }
  }

  private def mergeSourceWithUniqueKey(
      action: SourceDataAction,
      isInitialLoad: Boolean,
      isFullLoad: Boolean,
      dfNewData: DataFrame,
      dfOldDataAsOption: Option[DataFrame],
      dfAllMergeKeysWithServiceColumnsAsOption: Option[DataFrame],
      effectiveDate: Date,
      appLog: Logger): DataFrame = {

    val spark = SparkSession.builder().getOrCreate() // this gets previously created session

    val effDateYYYY_MM_DD = getDateFormatted(effectiveDate, "yyyy-MM-dd")
    val prevEffDateYYYY_MM_DD = getDateFormatted(effectiveDate, "yyyy-MM-dd", -1)

    appLog.info("Running for effective date " + effDateYYYY_MM_DD)

    if (isInitialLoad) {
      appLog.info("Running initial load for versioned data source with unique keys")
      if (action.getSdaIsFileDestination) saveNewStgFile(action, dfNewData, appLog)
      dfNewData
    } else {
      appLog.info("Running subsequent load for versioned data source with unique keys")

      val dfStg = if (dfOldDataAsOption.isDefined) {
        dfOldDataAsOption.get
      } else if (action.getSdaIsFileDestinationParquet) {
        spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
          .load(action.getSdaDestinationFilePath)
      }
      else
        throw new RuntimeException("""Loader ERROR: Only Parquet destination is currently supported """)
      dfStg.createOrReplaceTempView("StgData") // This is the existing file that we want to amend to create a new one and replace the existing one with the new

      if (action.getSdaIsDebugDwLib) {
        dfStg.select(date_format(min(col(serviceCols("RowTimestamp"))), "yyyyMMdd hhmmss.SSS").as("min formatted"),
          date_format(max(col(serviceCols("RowTimestamp"))), "yyyyMMdd hhmmss.SSS").as("max formatted")).show(5)
        appLog.info("Stg count: " + dfStg.count())
      }

      // The goal  here is to create new file with needed changes and all rows that are valid
      // (as opposed to table based approach where you modify the existing table to have valid rows

      dfNewData
        .cache() // cache new data it is used repeatedly here
        .createOrReplaceTempView("NewData")
      // for the incremental load we may have a separate file with all current keys,
      // if new data is not incremental it will have all keys

      // Incremental load
      if (!isFullLoad) {
        require(dfAllMergeKeysWithServiceColumnsAsOption.isDefined == true, """Loader ERROR: Incremental load requires defined dfAllMergeKeysAsOption """)
        require(action.getSdaMergeKeysList == action.getSdaPrimaryKeysList, """Loader ERROR: Merge and Primary keys must be the same for the incremental load""")
        dfAllMergeKeysWithServiceColumnsAsOption
          .get
          .createOrReplaceTempView("AllMergeKeys")
      }


      /**
       * At this point we have two or three views - depending on whther it is full or incremental load -
       * that are used in the code to produce a new result:
       * "StgData" - the existing file
       * "NewData" - new data: can be complete set if full load or changes only if incremental load
       * "AllMergeKeys" - the set of merge key for incremental load
       */

      val dfStgNew = if (action.getSdaIsVersioned) { // create versioned result
        createVersionedResultForSourceWithUniqueKey(action, isFullLoad, effDateYYYY_MM_DD, prevEffDateYYYY_MM_DD, appLog)
      }
      else { // create non-versioned result
        createNonVersionedResultForSourceWithUniqueKey(action, isFullLoad, appLog)
      }
      dfStgNew
    }

  }

  private def mergeSourceChanges(
      action: SourceDataAction,
      dfNewData: DataFrame,
      dfAllMergeKeys: DataFrame,
      dfOldDataAsOption: Option[DataFrame],
      effectiveDate: Date,
      appLog: Logger): DataFrame = {

    val spark = SparkSession.builder().getOrCreate() // this gets previously created session

    dfNewData
      .cache() // cache new data it is used repeatedly here
      .createOrReplaceTempView("NewData")

    val dfStg = if (dfOldDataAsOption.isDefined) {
      dfOldDataAsOption.get
    } else {
      throw new RuntimeException("""Loader ERROR: File based source to merge changes currently not supported """)
    }
    dfStg.createOrReplaceTempView("StgData") // This is the existing file that we want to amend to create a new one and replace the existing one with the new

    val mergeKeys = action.getSdaMergeKeysList
    dfAllMergeKeys
      .withColumn(serviceCols("RowMergeKey"), concatColumns(struct(mergeKeys.head, mergeKeys.tail: _*)))
      .createOrReplaceTempView("AllMergeKeys")


    // Step 1. Create un-changed data
    val sqlUnchanged
    =
      s"""
         | SELECT stg.*
         | FROM StgData AS stg
         |   INNER JOIN AllMergeKeys AS mergeKeys ON   stg.${serviceCols("RowMergeKey")} = mergeKeys.${serviceCols("RowMergeKey")}
         | WHERE NOT EXISTS (  SELECT 1
         |                     FROM NewData AS new
         |                     WHERE stg.${serviceCols("RowMergeKey")} = new.${serviceCols("RowMergeKey")} )
         |    """.stripMargin
    appLog.info("  Creating data frame with un-changed data on merge changes:\n" + sqlUnchanged)
    val dfUnchanged = spark.sql(sqlUnchanged)
    if (action.getSdaIsDebugDwLib) {
      dfUnchanged.show(3)
    }

    // Finally merge all data frames to create a final copy
    // For changed and deleted rows on the expire rows by setting new EffectiveDateEnd
    // "EffectiveDateEnd" column must be the last one (unless dropping a column in the middle is as efficient as at the end)
    // This is the result of the "else" -- not an initial load
    val dfStgNew = dfUnchanged
      .union(dfNewData)

    if (action.getSdaIsDebugDwLib) {
      appLog.info("Older Versions rows count: " + dfUnchanged.count())
    }

    if (action.getSdaIsFileDestination) saveNewStgFile(action, dfStgNew, appLog)
    dfStgNew
  }

  private def createVersionedResultForSourceWithUniqueKey(action: SourceDataAction, isFullLoad: Boolean, effDateYYYY_MM_DD: String, prevEffDateYYYY_MM_DD: String, appLog: Logger): DataFrame = {

    val spark = SparkSession.builder().getOrCreate() // this gets previously created session

    // Once we create the first df that eventually will be saved, spark can start saving it, thus erasing the current
    // file from the disk. That makes the file unavailable for the next operations that uses the same source and the
    // way spark works the file will be re-read for those. But because after creating dfStgOlderVersions spark can
    // start saving it, it will erase the original. For this reason save first in the new directory then delete
    // the old and rename new to old
    //
    // Also we use the same latest version over and over. For this reason
    // cache the latest version of the data before starting to create dataframes for the output.
    // On re-run some previously processed records for this effective date may have end date with this effective date
    // EffectiveDateEnd logic excludes old records that expired before this effective date, i.e.,
    // i want to include records that expired on "$prevEffDateYYYY_MM_DD", because on re-run that may change
    // Also EffectiveDateEnd >= "$prevEffDateYYYY_MM_DD" condition excludes records that expired before this effective date
    val sqlLatestVersionExcludingCurrentEffDate
    =
    s"""| WITH StgTableWithVersionNumber
        | AS
        | ( SELECT ROW_NUMBER() OVER ( PARTITION BY ${serviceCols("RowUniqueKey")} ORDER BY ${serviceCols("EffectiveDateStart")} DESC ) AS ReverseVersionNumber,
        |   *
        |   FROM StgData
        |   WHERE     ${serviceCols("EffectiveDateStart")} != "$effDateYYYY_MM_DD"
        |         AND ${serviceCols("EffectiveDateEnd")} >= "$prevEffDateYYYY_MM_DD"
        | )
        | SELECT *
        | FROM StgTableWithVersionNumber
        | WHERE ReverseVersionNumber = 1
        |   """.stripMargin
    appLog.info("  Creating data frame with most recent version excluding current effective date. It will be cached:\n" + sqlLatestVersionExcludingCurrentEffDate)
    val dfStgLatestVersionExcludingCurrentEffDate = spark.sql(sqlLatestVersionExcludingCurrentEffDate)
      .drop("ReverseVersionNumber")
    // .cache()  // caching was taken 10 extra min locally for a wide ~8MM row file on incremental load
    if (action.getSdaIsDebugDwLib) {
      appLog.info("Most recent version excluding current effective date rows count (this is cached): " + dfStgLatestVersionExcludingCurrentEffDate.count())
    }
    dfStgLatestVersionExcludingCurrentEffDate.createOrReplaceTempView("LatestVersionExcludingCurrentEffDate") // need this as view to determine new versions
    if (action.getSdaIsDebugDwLib) {
      dfStgLatestVersionExcludingCurrentEffDate.show(3)
    }

    // Step 1. Create df from existing Stg file with all versions which do not change
    //          These include all versions prior to the latest excluding versions with today's EffectiveDateStart
    //          which can be in the file if this is a re-run
    //          This result set will have all records whose EffectiveDateEnd != "2099-01-01"
    val sqlOlderVersions // there is at least one more recent version for all rows selected by this SQL or the versions expired before thi effective date.These versions will not change
    =
    s"""
       | SELECT *
       | FROM StgData
       | WHERE ${serviceCols("EffectiveDateEnd")} < "$prevEffDateYYYY_MM_DD"
       |   """.stripMargin
    appLog.info("  Creating data frame with older versions that will not change:\n" + sqlOlderVersions)
    val dfStgOlderVersions = spark.sql(sqlOlderVersions)
    if (action.getSdaIsDebugDwLib) {
      dfStgOlderVersions.show(3)
    }


    // It is the same code for incremental load as for full load. We cannot change the sql for incremental
    // load since NewData will have changed records and new records
    // On incremental load NewData will be just smaller. Also merge keys are the same as primary key - that is enforced above
    val sqlLatestVersionsChanged // on re-run some records may already have EffectiveDateEnd equal to previous day. Some other would have 2099-01-01 as EffectiveDateEnd
    =
    s"""
       | SELECT stg.*
       | FROM LatestVersionExcludingCurrentEffDate AS stg
       |   INNER JOIN NewData AS new ON   stg.${serviceCols("RowUniqueKey")} = new.${serviceCols("RowUniqueKey")}
       |                              AND stg.${serviceCols("RowHash")} != new.${serviceCols("RowHash")}
       |    """.stripMargin
    appLog.info("  Creating data frame with latest versions that will change:\n" + sqlLatestVersionsChanged)
    val dfStgLatestVersionsChanged = spark.sql(sqlLatestVersionsChanged)
      .drop({
        serviceCols("EffectiveDateEnd")
      })
      .withColumn({
        serviceCols("EffectiveDateEnd")
      }, to_date(lit(prevEffDateYYYY_MM_DD)))
    if (action.getSdaIsDebugDwLib) {
      dfStgLatestVersionsChanged.show(3)
    }

    // On incremental load of versioned data merge keys are the same as primary key - that is enforced above
    val sqlLatestVersionsDeleted // on re-run some records may already have EffectiveDateEnd equal to previous day. Some other would have 2099-01-01 as EffectiveDateEnd
    =
      s"""
         | SELECT stg.*
         | FROM LatestVersionExcludingCurrentEffDate AS stg
         | WHERE NOT EXISTS (SELECT 1
         |                     FROM ${if (isFullLoad) "NewData" else "AllMergeKeys"} AS new
         |                     WHERE   stg.${serviceCols("RowMergeKey")} = new.${serviceCols(if (isFullLoad) "RowUniqueKey" else "RowMergeKey")} )
         |    """.stripMargin
    appLog.info("  Creating data frame with latest versions that are deleted:\n" + sqlLatestVersionsDeleted)
    val dfStgLatestVersionsDeleted = spark.sql(sqlLatestVersionsDeleted)
      .drop({
        serviceCols("EffectiveDateEnd")
      })
      .withColumn({
        serviceCols("EffectiveDateEnd")
      }, to_date(lit(prevEffDateYYYY_MM_DD)))
    if (action.getSdaIsDebugDwLib) {
      dfStgLatestVersionsDeleted.show(3)
    }


    // on re-run some records may have EffectiveDateEnd equal to previous day. We would need to set all EffectiveDateEnd values to 2099-01-01
    val sqlLatestVersionsNotChanged = if (isFullLoad) {
      s"""
         | SELECT stg.*
         | FROM LatestVersionExcludingCurrentEffDate AS stg
         |   INNER JOIN NewData AS new ON   stg.${serviceCols("RowUniqueKey")} = new.${serviceCols("RowUniqueKey")}
         |                              AND stg.${serviceCols("RowHash")} = new.${serviceCols("RowHash")}
         |    """.stripMargin
    }
    else { // on incremental load NewData only has changes and new rows
      s"""
         | SELECT stg.*
         | FROM LatestVersionExcludingCurrentEffDate AS stg
         |   INNER JOIN AllMergeKeys AS keys ON stg.${serviceCols("RowMergeKey")} = keys.${serviceCols("RowMergeKey")}
         | WHERE NOT EXISTS (SELECT 1
         |                     FROM NewData AS new
         |                     WHERE stg.${serviceCols("RowMergeKey")} = new.${serviceCols("RowMergeKey")} )
         |    """.stripMargin
    }

    appLog.info("  Creating data frame with latest versions that will not change:\n" + sqlLatestVersionsNotChanged)
    val dfStgLatestVersionsNotChanged = spark.sql(sqlLatestVersionsNotChanged)
      .drop(serviceCols("EffectiveDateEnd"))
      .withColumn(serviceCols("EffectiveDateEnd"), to_date(lit(farFutureDateYYYY_MM_DD)))
    if (action.getSdaIsDebugDwLib) {
      dfStgLatestVersionsNotChanged.show(3)
    }

    // Now include new unique key and new versions for the existing ones
    // new data is still missing three columns - Version , start and end date
    val sqlNewVersions
    =
    s"""
       | SELECT new.* , 1 AS ${serviceCols("Version")}   -- set version to 1 for rows with new unique key
       | FROM NewData AS new
       | WHERE NOT EXISTS (  SELECT 1
       |                     FROM LatestVersionExcludingCurrentEffDate AS notChanged
       |                     WHERE notChanged.${serviceCols("RowUniqueKey")} = new.${serviceCols("RowUniqueKey")} )
       | UNION ALL
       | SELECT new.*, stg.${serviceCols("Version")} + 1 AS ${serviceCols("Version")} -- for new versions of existing rows bump the version by 1
       | FROM LatestVersionExcludingCurrentEffDate AS stg
       |    INNER JOIN NewData AS new ON   stg.${serviceCols("RowUniqueKey")} = new.${serviceCols("RowUniqueKey")}
       |                               AND stg.${serviceCols("RowHash")} != new.${serviceCols("RowHash")}
       |    """.stripMargin
    val dfNewVersions = spark.sql(sqlNewVersions)
      .withColumn(serviceCols("EffectiveDateStart"), to_date(lit(effDateYYYY_MM_DD)))
      .withColumn(serviceCols("EffectiveDateEnd"), to_date(lit(farFutureDateYYYY_MM_DD))) // This column must be the last one. It will be replaced during versioning process

    if (action.getSdaIsDebugDwLib) {
      dfNewVersions.show(3)
    }

    // Finally merge all data frames to create a final copy
    // For changed and deleted rows on the expire rows by setting new EffectiveDateEnd
    // "EffectiveDateEnd" column must be the last one (unless dropping a column in the middle is as efficient as at the end)
    // This is the result of the "else" -- not an initial load
    val dfStgNew = dfStgOlderVersions
      .union(dfStgLatestVersionsChanged)
      .union(dfStgLatestVersionsDeleted)
      .union(dfStgLatestVersionsNotChanged)
      .union(dfNewVersions)

    if (action.getSdaIsDebugDwLib) {
      appLog.info("Older Versions rows count: " + dfStgOlderVersions.count())
      appLog.info("Latest Versions Changed rows count: " + dfStgLatestVersionsChanged.count())
      appLog.info("Latest Versions Deleted rows count: " + dfStgLatestVersionsDeleted.count())
      appLog.info("Latest Versions Not Change rows count: " + dfStgLatestVersionsNotChanged.count())
      appLog.info("New versions rows count: " + dfNewVersions.count())
    }

    if (action.getSdaIsFileDestination) saveNewStgFile(action, dfStgNew, appLog)
    dfStgNew
  }

  private def createNonVersionedResultForSourceWithUniqueKey(action: SourceDataAction, isFullLoad: Boolean, appLog: Logger): DataFrame = {

    val spark = SparkSession.builder().getOrCreate() // this gets previously created session

    // Step 1. Create changed data
    val sqlChanged
    =
      s"""
         | SELECT stg.*
         | FROM StgData AS stg
         |   INNER JOIN NewData AS new ON   stg.${serviceCols("RowUniqueKey")} = new.${serviceCols("RowUniqueKey")}
         |                              AND stg.${serviceCols("RowHash")} != new.${serviceCols("RowHash")}
         |    """.stripMargin
    appLog.info("  Creating data frame with changed data:\n" + sqlChanged)
    val dfChanged = spark.sql(sqlChanged)
    if (action.getSdaIsDebugDwLib) {
      dfChanged.show(3)
    }

    // Step 2. Create not changed data excluding deleted records
    val sqlNotChangedExcludingDeleted = if (isFullLoad) {
      s"""
         | SELECT stg.*
         | FROM StgData AS stg
         |   INNER JOIN NewData AS new ON   stg.${serviceCols("RowUniqueKey")} = new.${serviceCols("RowUniqueKey")}
         |                              AND stg.${serviceCols("RowHash")} = new.${serviceCols("RowHash")}
         |
          """.stripMargin
    }
    else { // Incremental load
      s"""
         | SELECT stg.*
         | FROM StgData AS stg
         |   INNER JOIN AllMergeKeys AS keys ON stg.${serviceCols("RowMergeKey")} = keys.${serviceCols("RowMergeKey")}
         | WHERE NOT EXISTS (SELECT 1
         |                     FROM NewData AS new
         |                     WHERE stg.${serviceCols("RowMergeKey")} = new.${serviceCols("RowMergeKey")} )
          """.stripMargin
    }

    appLog.info("  Creating data frame with not changed records:\n" + sqlNotChangedExcludingDeleted)
    val dfStgNotChangedExcludingDeleted = spark.sql(sqlNotChangedExcludingDeleted)
    if (action.getSdaIsDebugDwLib) {
      dfStgNotChangedExcludingDeleted.show(3)
    }

    // Step 3. Create new records
    val sqlNew
    =
      s"""
         | SELECT new.*
         | FROM NewData AS new
         | WHERE NOT EXISTS (  SELECT 1
         |                     FROM StgData AS stg
         |                     WHERE stg.${serviceCols("RowUniqueKey")} = new.${serviceCols("RowUniqueKey")} ) """.stripMargin
    val dfNew = spark.sql(sqlNew)

    if (action.getSdaIsDebugDwLib) {
      dfNew.show(3)
    }

    // Finally merge all data frames to create a final copy
    // For changed and deleted rows on the expire rows by setting new EffectiveDateEnd
    // "EffectiveDateEnd" column must be the last one (unless dropping a column in the middle is as efficient as at the end)
    // This is the result of the "else" -- not an initial load
    val dfStgNew = dfChanged
      .union(dfStgNotChangedExcludingDeleted)
      .union(dfNew)

    if (action.getSdaIsDebugDwLib) {
      appLog.info("Older Versions rows count: " + dfChanged.count())
      appLog.info("Latest Versions Not Change rows count: " + dfStgNotChangedExcludingDeleted.count())
      appLog.info("New versions rows count: " + dfNew.count())
    }

    if (action.getSdaIsFileDestination) saveNewStgFile(action, dfStgNew, appLog)
    dfStgNew
  }

  private def mergeSourceWithNoUniqueKey(action: SourceDataAction, isInitialLoad: Boolean, dfNewData: DataFrame, dfOldDataAsOption: Option[DataFrame], effectiveDate: Date, appLog: Logger): DataFrame = {

    val spark = SparkSession.builder().getOrCreate() // this gets previously created session

    val effDateYYYY_MM_DD = getDateFormatted(effectiveDate, "yyyy-MM-dd")

    appLog.info("Running for effective date " + effDateYYYY_MM_DD)

    val dfResult = if (!action.getSdaIsVersioned) {
      appLog.info("Running load for non-versioned data source with no unique keys")
      if (action.getSdaIsFileDestination) saveNewStgFile(action, dfNewData, appLog) // Just create anew file by overwriting the existing one
      dfNewData
    } else if (isInitialLoad) { // i.e., versioned and initial load
      appLog.info("Running initial load for versioned data source with no unique keys")
      if (action.getSdaIsFileDestination) saveNewStgFile(action, dfNewData, appLog)
      dfNewData
    } else { // i.e., versioned and subsequent load
      appLog.info("Running subsequent load")

      val dfStg = if (dfOldDataAsOption.isDefined) {
        dfOldDataAsOption.get
      } else if (action.getSdaIsFileDestinationParquet) {
        spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
          .load(action.getSdaDestinationFilePath)
      } else {
        throw new RuntimeException("""Loader ERROR: Only Parquet destination is currently supported """)
      }

      if (action.getSdaIsDebugDwLib) {
        appLog.info("Stg count: " + dfStg.count())
      }


      // The goal  here is to create new file with needed changes and all rows that are valid
      // (as opposed to table based approach where you modify the existing table to have valid rows

      dfStg.createOrReplaceTempView("StgData") // This is the existing file that we want to amend to create a new one and replace the existing one with the new

      val sqlExcludingCurrentEffDate
      =
        s"""
           | SELECT *
           | FROM StgData
           | WHERE ${serviceCols("EffectiveDate")} != "$effDateYYYY_MM_DD"
           |   """.stripMargin
      appLog.info("  Creating data frame with data excluding current effective date:\n" + sqlExcludingCurrentEffDate)
      val dfStgExcludingCurrentEffDate = spark.sql(sqlExcludingCurrentEffDate)

      if (action.getSdaIsDebugDwLib) {
        appLog.info("Staging data excluding current effective date rows count (this is cached): " + dfStgExcludingCurrentEffDate.count())
        dfStgExcludingCurrentEffDate.show(3)
      }


      // Finally merge all data frames to create a final copy
      // For changed and deleted rows on the expire rows by setting new EffectiveDateEnd
      // "EffectiveDateEnd" column must be the last one (unless dropping a column in the middle is as efficient as at the end)
      // This is the result of the "else" -- not an initial load
      val dfStgNew = dfStgExcludingCurrentEffDate
        .union(dfNewData)

      if (action.getSdaIsDebugDwLib) {
        appLog.info("Staging excluding current effective date rows count: " + dfNewData.count())
        appLog.info("New data rows count: " + dfNewData.count())
      }

      if (action.getSdaIsFileDestination) saveNewStgFile(action, dfStgNew, appLog)
      dfStgNew
    }
    dfResult
  }

  private def saveNewStgFile(action: SourceDataAction, dfStgNew: DataFrame, appLog: Logger): Unit = {
    val spark = SparkSession.builder().getOrCreate() // this gets previously created session

    // Before saving the file set the service columns to nullable.
    // Drill and other parquet readers are complaining
    val dfStgNewWithNullableServiceCols = dfStgNew.setNullableStateForAllColumns(true)

    if (!action.getSdaIsFileDestinationParquet) {
      throw new RuntimeException("""Loader ERROR: Only Parquet destination is currently supported """)
    }

    if (action.getSdaIsDebugDwLib) {
      dfStgNewWithNullableServiceCols.printSchema()
      appLog.info("Saved new version of the staging file with row count:" + dfStgNew.count())
    }

    if (action.getSdaIsSavePreviousVersionOfDestinationFile) {
      FileHelper.saveDataFrameAsParquet(dfStgNewWithNullableServiceCols, action.getSdaDestinationFilePath, action.getSdaFileDestinationSavePreviousVersionAs)
    }
    else {
      FileHelper.saveDataFrameAsParquet(dfStgNewWithNullableServiceCols, action.getSdaDestinationFilePath)
    }
  }

}
