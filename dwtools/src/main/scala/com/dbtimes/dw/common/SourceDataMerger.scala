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

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.dbtimes.dw.common.DataFrameHelper.{DataFrameImplicits, concatColumns}
import com.dbtimes.dw.common.MiscHelper.getDateFormatted
import LogFile.{dwlogger => appLog}
// import org.apache.log4j.Logger
// import org.slf4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.Observation
import scala.collection.immutable.ListMap



private[dw] trait SourceDataMerger {

  // columns added to the data source. The names can be prefixed with arbitrary string to avoid possible
  // collision with actual data source columns which can have the same column name.
  private val metadataColsBaseNames: Map[String, String] = Map(
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
  private var metadataCols: Map[String, String] = Map.empty

  private val farFutureDateYYYY_MM_DD = "2099-01-01"

  protected def mergeSourceData(action: SourceDataAction, dfNewData: DataFrame, effectiveDate: Date ): DataFrame = {
    mergeSourceData(action, true, dfNewData, false, None, None, effectiveDate)
  }

  protected def mergeSourceDataChanges(action: SourceDataAction, dfNewData: DataFrame, isNewDataPreparedForMerge: Boolean, dfAllMergeKeys: DataFrame, dfOldData: DataFrame, effectiveDate: Date): DataFrame = {
    mergeSourceData(action, false, dfNewData, isNewDataPreparedForMerge, Some(dfAllMergeKeys), Some(dfOldData), effectiveDate)
  }

  protected def mergeSourceDataChanges(action: SourceDataAction, dfNewData: DataFrame, isNewDataPreparedForMerge: Boolean, dfAllMergeKeys: DataFrame, effectiveDate: Date): DataFrame = {
    mergeSourceData(action, false, dfNewData, isNewDataPreparedForMerge, Some(dfAllMergeKeys), dfOldDataAsOption = None, effectiveDate)
  }

  /**
   * mergeSourceData
   * mergeSourceWithUniqueKey
   * createVersionedResultForSourceWithUniqueKey
   * createNonVersionedResultForSourceWithUniqueKey
   * mergeSourceChanges
   * mergeSourceWithNoUniqueKey
   *
   * @param action
   * @param isFullLoad             - it is full vs. incremental. Incremental means that the new data is changes
   *                               only, not the whole set. If it is the whole set  it is full. Incremental load
   *                               cannot be initial. Subsequent load can be full or incremental.
   * @param dfNewData
   * @param dfAllMergeKeysAsOption -- merge keys can be defined without defining unique keys,
   *                               or they can be a separate set of columns, or be the same a unique keys,
   *                               or a subset of unique keys
   * @param dfOldDataAsOption      -- if not defined it is not yet loaded from the source(as opposed not valid or available)
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
      effectiveDate: Date): DataFrame = {

    val spark = SparkSession.builder().getOrCreate() // this gets previously created session
    metadataCols = metadataColsBaseNames.transform { (key, value) => action.getMetadataColumnPrefix + value } // this is deprecated in later version. Use mapValuesInPlace

    val isInitialLoad = action.getIsInitialLoad

    // Read old data here - will need to do it in all merge scenarios anyway.
    // The other reason for reading it here is that new data can be empty and without a schema from some dbms sources, like MongoDB.
    // In that case set the schema to the one of old data
    val dfStgOldAsOption: Option[DataFrame] = if (isInitialLoad) {
      None
    }
    else {
      if (dfOldDataAsOption.isDefined) {
        dfOldDataAsOption
      } else if (action.getIsFileDestinationParquet) {
        Some(spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
          .load(action.getDestinationFilePath))
      }
      else
        throw new RuntimeException("""Loader ERROR: Only Parquet destination is currently supported """)
    }

    val dfNewDataPrepared = if (isNewDataPreparedForMerge)
      dfNewData
    else {
      addMetadataColumnsAndPrepareForMerge(action, dfNewData, dfStgOldAsOption, effectiveDate)
    }

    if (action.getIsDebugDwLib) {
      dfNewDataPrepared.show()
      dfNewDataPrepared.printSchema()
    }

    val dfAllMergeKeysWithMetadataColumnsAsOption = if (dfAllMergeKeysAsOption.isDefined) {
      val mergeKeys = action.getMergeKeysList
      val dfAllMergeKeysWithMetadataColumns = dfAllMergeKeysAsOption.get
        // Metadata columns
        // add composite unique key. The Dataset must have at least one primary key.
        .withColumn(metadataCols("RowMergeKey"), if (mergeKeys.isEmpty) lit(null).cast(StringType) else concatColumns(struct(mergeKeys.head, mergeKeys.tail: _*)))

      Some(dfAllMergeKeysWithMetadataColumns)
    }
    else {
      None
    }

    val sourceHasPrimaryKey = !action.getPrimaryKeysList.isEmpty
    val sourceHasMergeKey = !action.getMergeKeysList.isEmpty

    val dfResult = if (sourceHasPrimaryKey && (isFullLoad || !isFullLoad && action.getMergeKeysList == action.getPrimaryKeysList)) {
      mergeSourceWithUniqueKey(action, isInitialLoad, isFullLoad, dfNewDataPrepared, dfStgOldAsOption, dfAllMergeKeysWithMetadataColumnsAsOption, effectiveDate)
    }
    else if (sourceHasMergeKey && !isFullLoad && !action.getIsVersioned) { // This case cannot currently happen for source loads because we require versioned data for incremental load.
      // Incremental load: merge keys are different from unique keys
      require(isInitialLoad == false, """Loader ERROR: Incremental load cannot be done on initial load """)
      require(dfAllMergeKeysAsOption.isDefined == true, """Loader ERROR: Incremental load requires defined dfAllMergeKeysAsOption """)
      require(action.getMergeKeysList.isEmpty == false, """Loader ERROR: Incremental load requires a list of one or more Merge Key  """)
      mergeSourceChanges(action, dfNewDataPrepared, dfAllMergeKeysAsOption.get, dfStgOldAsOption, effectiveDate)
    }
    else if (sourceHasPrimaryKey || sourceHasMergeKey) {
      throw new RuntimeException("""Loader Configuration ERROR: Unsupported combination of load attributes """)
    }
    else {
      mergeSourceWithNoUniqueKey(action, isInitialLoad, dfNewDataPrepared, dfStgOldAsOption, effectiveDate)
    }

    // Set all columns to nullable.
    // Drill and other parquet readers are complaining
    dfResult.setNullableStateForAllColumns(true)
  }

  /**
   *
   * @param action
   * @param dfNewData
   * @param dfStgOldAsOption - old data is only used for incremental load when the new data does not have any schema.
   *                         That would be the case for Mongo DB empty result when MongDB driver cannot create any schema.
   * @param effectiveDate
   * @param appLog
   * @return
   */
  protected def addMetadataColumnsAndPrepareForMerge(action: SourceDataAction, dfNewData: DataFrame, dfStgOldAsOption: Option[DataFrame], effectiveDate: Date): DataFrame = {

    // For non-initial load set all metadata columns except for Version, EffectiveDateStart and EffectiveDateEnd
    // These three columns are only set for initial load

    val isInitialLoad = action.getIsInitialLoad

    // If schema is zero length - can be the case for MongoDB when the result is empty data set -
    // use old data to set the schema
    if (dfNewData.schema.length == 0) {
      if (!isInitialLoad) {
        require(dfStgOldAsOption.isDefined) // old data has to be defined for non-initial load
        val oldDataSchema = dfStgOldAsOption.get.schema

        val metadataColumnsToDrop = Set(metadataCols("Version"), metadataCols("EffectiveDateStart"), metadataCols("EffectiveDateEnd"))

        val oldDataSchemaWithoutVersionColumn = StructType(
          oldDataSchema.fields.filterNot(item => metadataColumnsToDrop(item.name))
        )
        val dfNewDataWithMetadataColumns = dfNewData.setNewSchema(oldDataSchemaWithoutVersionColumn)

        if (action.getIsDebugDwLib) {
          dfNewDataWithMetadataColumns.show()
          dfNewDataWithMetadataColumns.printSchema()
        }

        dfNewDataWithMetadataColumns
      }
      else {
        // initial load and the schema is empty
        throw new RuntimeException("""Loader ERROR: The data is empty with no schema on initial load. Check your data source and query""")
      }
    }
    else {
      metadataCols = metadataColsBaseNames.transform { (key, value) => action.getMetadataColumnPrefix + value } // this is deprecated in later version. Use mapValuesInPlace

      val effDateYYYY_MM_DD = getDateFormatted(effectiveDate, "yyyy-MM-dd")
      val primaryKeys = action.getPrimaryKeysList
      val mergeKeys = action.getMergeKeysList
      val columnsToExcludeFromVersioning = action.getExcludeFromVersioningColumnList
      val sourceHasPrimaryKey = !action.getPrimaryKeysList.isEmpty
      val sourceHasMergeKey = !action.getMergeKeysList.isEmpty
      // Exclude from RowHash columns with attribute "isExcludeFromVersioning" : "true"
      val columnsForRowHash = dfNewData.columns.toList.diff(columnsToExcludeFromVersioning)


      val dfNewDataPrepared = if (sourceHasPrimaryKey || sourceHasMergeKey) {
        val dfNewDataWithMetadataColumnsExceptVersion = dfNewData
          // Metadata columns
          // add composite unique key. The Dataset must have at least one primary key.
          .withColumns(ListMap( // ListMap preserves the order of columns
            metadataCols("RowUniqueKey") -> {
              if (primaryKeys.isEmpty) lit(null).cast(StringType) else concatColumns(struct(primaryKeys.head, primaryKeys.tail: _*))
            },
            metadataCols("RowMergeKey") -> {
              if (mergeKeys.isEmpty) lit(null).cast(StringType) else concatColumns(struct(mergeKeys.head, mergeKeys.tail: _*))
            },
            // Before there was expression struct("*") which unintentionally included two just added metadata columns
            // "RowUniqueKey" and "RowMergeKey". Those columns were not causing an issue but they are not needed in RowHash.
            metadataCols("RowHash") -> {
              val value = md5(concatColumns(struct(columnsForRowHash.head, columnsForRowHash.tail: _*)));
              when(value.isNotNull, value).otherwise(lit(null))
            },
            metadataCols("RowTimestamp") -> {
              val value = current_timestamp(); // in DB this is ROWVERSION. Here we assume that there is some time between loads that will allow to use timestamp to determine what changed since last load. !! important: we need to maintan this for any changes to the row, like expiring a row
              when(value.isNotNull, value).otherwise(lit(null))
            },
            metadataCols("CreatedOn") -> {
              val value = current_timestamp();
              when(value.isNotNull, value).otherwise(lit(null))
            },
            metadataCols("CreatedBy") -> {
              val value = lit(System.getProperty("user.name")); // identity of who runs the job
              when(value.isNotNull, value).otherwise(lit(null))
            }
          ))

        /*
                  .withColumn(metadataCols("RowUniqueKey"), if (primaryKeys.isEmpty) lit(null).cast(StringType) else concatColumns(struct(primaryKeys.head, primaryKeys.tail: _*)))
                  .withColumn(metadataCols("RowMergeKey"), if (mergeKeys.isEmpty) lit(null).cast(StringType) else concatColumns(struct(mergeKeys.head, mergeKeys.tail: _*)))
                  // Before there was expression struct("*") which unintentionally included two just added metadata columns
                  // "RowUniqueKey" and "RowMergeKey". Those columns were not causing an issue but they are not needed in RowHash.
                  .withColumn(metadataCols("RowHash"), md5(concatColumns(struct(columnsForRowHash.head, columnsForRowHash.tail: _*))))
                  .withColumn(metadataCols("RowTimestamp"), current_timestamp()) // in DB this is ROWVERSION. Here we assume that there is some time between loads that will allow to use timestamp to determine what changed since last load. !! important: we need to maintan this for any changes to the row, like expiring a row
                  .withColumn(metadataCols("CreatedOn"), current_timestamp())
                  .withColumn(metadataCols("CreatedBy"), lit(System.getProperty("user.name"))) // identity of who runs the job
        */

        if (action.getIsDebugDwLib) {
          appLog.info("All rows count: " + dfNewDataWithMetadataColumnsExceptVersion.count())
        }

        // remove duplicates
        val dfNewNoDups = if (sourceHasPrimaryKey && action.getIsRemoveDuplicateRows) {
          dfNewDataWithMetadataColumnsExceptVersion.dropDuplicates(metadataCols("RowUniqueKey"))
        }
        else
          dfNewDataWithMetadataColumnsExceptVersion

        if (action.getIsDebugDwLib) {
          appLog.info("Distinct count: " + dfNewNoDups.count())
        }

        // Only add version on initial load
        val dfStgNew = if (isInitialLoad && sourceHasPrimaryKey && action.getIsVersioned) {
          appLog.info("Running initial load for versioned data source with unique keys")
          // Only add these metadata columns on initial load because the data will be saved after that without additional transformations
          // If this is subsequent load the same metadata columns will be added during processing
          dfNewNoDups
            .withColumns(ListMap( // ListMap preserves the order of columns
              metadataCols("Version") -> {
                val value = lit(1);
                when(value.isNotNull, value).otherwise(lit(null))
              },
              metadataCols("EffectiveDateStart") -> {
                val value = to_date(lit(effDateYYYY_MM_DD));
                when(value.isNotNull, value).otherwise(lit(null))
              },
              metadataCols("EffectiveDateEnd") -> {
                val value = to_date(lit(farFutureDateYYYY_MM_DD)); // This column must be the last one. It will be replaced during versioning process
                when(value.isNotNull, value).otherwise(lit(null))
              }
            ))

          /*
                      .withColumn(metadataCols("Version"), lit(1)) // set version to 1 for new data
                      .withColumn(metadataCols("EffectiveDateStart"), to_date(lit(effDateYYYY_MM_DD)))
                      .withColumn(metadataCols("EffectiveDateEnd"), to_date(lit(farFutureDateYYYY_MM_DD))) // This column must be the last one. It will be replaced during versioning process
          */
        } else {
          dfNewNoDups
        }
        dfStgNew
      }
      else { // source has neither primary or merge keys
        val dfNewDataWithMetadataColumns = dfNewData
          // Metadata columns
          .withColumns(ListMap( // ListMap preserves the order of columns
            metadataCols("RowTimestamp") -> {
              val value = current_timestamp(); // in DB this is ROWVERSION. Here we assume that there is some time between loads that will allow to use timestamp to determine what changed since last load. !! important: we need to maintan this for any changes to the row, like expiring a row
              when(value.isNotNull, value).otherwise(lit(null))
            },
            metadataCols("CreatedOn") -> {
              val value = current_timestamp();
              when(value.isNotNull, value).otherwise(lit(null))
            },
            metadataCols("CreatedBy") -> {
              val value = lit(System.getProperty("user.name")); // identity of who runs the job
              when(value.isNotNull, value).otherwise(lit(null))
            },
            metadataCols("EffectiveDate") -> {
              val value = to_date(lit(effDateYYYY_MM_DD)); // This column must be the last one. It will be replaced during versioning process
              when(value.isNotNull, value).otherwise(lit(null))
            }
          ))

        /*
                  // Metadata columns
                  .withColumn(metadataCols("RowTimestamp"), current_timestamp())
                  // in DB this is ROWVERSION. Here we assume that there is some time between loads that will allow to use timestamp to determine what changed since last load. !! important: we need to maintan this for any changes to the row, like expiring a row
                  .withColumn(metadataCols("CreatedOn"), current_timestamp())
                  .withColumn(metadataCols("CreatedBy"), lit(System.getProperty("user.name"))) // identity of who runs the job
                  .withColumn(metadataCols("EffectiveDate"), to_date(lit(effDateYYYY_MM_DD)))
        */

        if (action.getIsDebugDwLib) {
          appLog.info("All rows count: " + dfNewDataWithMetadataColumns.count())
        }
        dfNewDataWithMetadataColumns
      }

      dfNewDataPrepared
    }
  }

  protected def createLoadControlRecordObservation(
      action: SourceDataAction,
      timeStart: Long,
      effectiveDate: Date,
      df: DataFrame
  ): (DataFrame, Option[Observation]) = {


    if (action.getIsMaintainLoadControl) {
      val observationMetrics = Observation("DataframeMetrics")
      val isInitialLoad = action.getIsInitialLoad
      val sourceHasPrimaryKey = !action.getPrimaryKeysList.isEmpty
      val effDateYYYY_MM_DD = getDateFormatted(effectiveDate, "yyyy-MM-dd")
      val prevEffDateYYYY_MM_DD = getDateFormatted(effectiveDate, "yyyy-MM-dd", -1)
      val loadStartTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(timeStart)

      val dfObserved = df.observe(
        observationMetrics,
        max(col(metadataCols("RowTimestamp"))).as("maxRowTimestamp"),
        if (sourceHasPrimaryKey)
          count(lit(1)).as("countTotalAffectedRecords")
        else
          count(when(col(metadataCols("EffectiveDate")) === s"$effDateYYYY_MM_DD", 1)).as("countTotalAffectedRecords"),
        if (sourceHasPrimaryKey && !action.getIsVersioned)
          count(when(col(metadataCols("RowTimestamp")) < s"$loadStartTime", 1)).as("countUnchangedRecords")
        else
          lit(-1L).as("countUnchangedRecords"),
        if (sourceHasPrimaryKey && !action.getIsVersioned)
          count(when(col(metadataCols("RowTimestamp")) >= s"$loadStartTime", 1)).as("countNewAndChangedRecords")
        else
          lit(-1L).as("countNewAndChangedRecords"),
        if (sourceHasPrimaryKey && action.getIsVersioned)
          count(when(col(metadataCols("EffectiveDateEnd")) < s"$prevEffDateYYYY_MM_DD", 1)).as("countOlderVersions")
        else
          lit(-1L).as("countOlderVersions"),
        if (sourceHasPrimaryKey && action.getIsVersioned)
          count(when(col(metadataCols("EffectiveDateStart")) < s"$effDateYYYY_MM_DD" && col(metadataCols("EffectiveDateEnd")) === s"$farFutureDateYYYY_MM_DD", 1)).as("countCurrentUnchangedVersions")
        else
          lit(-1L).as("countCurrentUnchangedVersions"),
        if (sourceHasPrimaryKey && action.getIsVersioned)
          count(when(col(metadataCols("EffectiveDateStart")) < s"$effDateYYYY_MM_DD" && col(metadataCols("EffectiveDateEnd")) === s"$prevEffDateYYYY_MM_DD", 1)).as("countDeletedVersions")
        else
          lit(-1L).as("countDeletedVersions"),
        if (sourceHasPrimaryKey && action.getIsVersioned)
          count(when(col(metadataCols("EffectiveDateStart")) === s"$effDateYYYY_MM_DD" && col(metadataCols("EffectiveDateEnd")) === s"$farFutureDateYYYY_MM_DD" && col(metadataCols("Version")) === 1, 1)).as("countFirstVersions")
        else
          lit(-1L).as("countFirstVersions"),
        if (sourceHasPrimaryKey && action.getIsVersioned)
          count(when(col(metadataCols("EffectiveDateStart")) === s"$effDateYYYY_MM_DD" && col(metadataCols("EffectiveDateEnd")) === s"$farFutureDateYYYY_MM_DD" && col(metadataCols("Version")) > 1, 1)).as("countNewVersions")
        else
          lit(-1L).as("countNewVersions")
      )

      (dfObserved, Some(observationMetrics))
    }
    else {
      (df, None)
    }
  }

  protected def createLoadControlRecordFromFileDestinationParquet(
      action: SourceDataAction,
      timeStart: Long,
      effectiveDate: Date,
      isSuccess: Boolean,
      errorMessage: String,
      observationMetricsAsOption: Option[Observation] = None): Unit = {

    val isInitialLoad = action.getIsInitialLoad
    val sourceHasPrimaryKey = !action.getPrimaryKeysList.isEmpty
    val result = if (isSuccess) "success" else "fail"

    if (action.getIsMaintainLoadControl) {
      val spark = SparkSession.builder().getOrCreate() // this gets previously created session

      val loadDurationSec = (Calendar.getInstance().getTimeInMillis() - timeStart).toDouble / 1e3 // seconds

      val sourceDescriptionAbbreviated = if (action.getSourceDescription.length() > 80) {
        action.getSourceDescription.substring(0, 37)
          .concat("...")
          .concat(action.getSourceDescription.substring(action.getSourceDescription.length() - 39)) // last 39 characters
      }
      else {
        action.getSourceDescription
      }


      val loadDescription =
        if (isInitialLoad && sourceHasPrimaryKey && action.getIsVersioned) "Initial load of source with unique key to versioned destination"
        else if (isInitialLoad && sourceHasPrimaryKey && !action.getIsVersioned) "Initial load of source with unique key to non-versioned destination"
        else if (isInitialLoad && !sourceHasPrimaryKey && action.getIsVersioned) "Initial load of source with no unique key to versioned destination"
        else if (isInitialLoad && !sourceHasPrimaryKey && !action.getIsVersioned) "Initial load of source with no unique key to non-versioned destination"
        else if (!isInitialLoad && sourceHasPrimaryKey && action.getIsVersioned) "Subsequent load of source with unique key to versioned destination"
        else if (!isInitialLoad && sourceHasPrimaryKey && !action.getIsVersioned) "Subsequent load of source with unique key to non-versioned destination"
        else if (!isInitialLoad && !sourceHasPrimaryKey && action.getIsVersioned) "Subsequent load of source with no unique key to versioned destination"
        else if (!isInitialLoad && !sourceHasPrimaryKey && !action.getIsVersioned) "Subsequent load of source with no unique key to non-versioned destination"
        else ""

      val loadControlRow = if (!isSuccess) {
        Seq(
          Row(
            action.getName, // "LoadName"
            action.getSourceTypeDescription, // "SourceType"
            sourceDescriptionAbbreviated, // "ProcessedSource"
            action.getDestinationTypeDescription, // "DestinationType"
            action.getDestinationDescription, // "Destination"
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
        val loadStartTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(timeStart)

        val metrics: Map[String, Any] = if (observationMetricsAsOption.isDefined
        // && observationMetricsAsOption.get.future.isCompleted  // this is for Scala 2.13. In scala 2.12 future is not available so we will just block until the observation is completed
        )
          observationMetricsAsOption.get.get
        else
          Map.empty

        val maxRowTimestamp = if (metrics.nonEmpty)
          metrics("maxRowTimestamp").asInstanceOf[java.sql.Timestamp]
        else
          spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
            .load(action.getDestinationFilePath)
            .agg(max(metadataCols("RowTimestamp"))).head.getTimestamp(0)

        val countTotalAffectedRecords = if (metrics.nonEmpty)
          metrics("countTotalAffectedRecords").asInstanceOf[Long]
        else {
          if (sourceHasPrimaryKey)
            spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
              .load(action.getDestinationFilePath)
              .count()
          else
            spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
              .load(action.getDestinationFilePath)
              .where(s"""${metadataCols("EffectiveDate")} = "$effDateYYYY_MM_DD" """)
              .count()
        }

        val countUnchangedRecords = if (metrics.nonEmpty)
          metrics("countUnchangedRecords").asInstanceOf[Long]
        else {
          if (sourceHasPrimaryKey && !action.getIsVersioned)
            spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
              .load(action.getDestinationFilePath)
              .where(s"""${metadataCols("RowTimestamp")} < "$loadStartTime" """)
              .count()
          else
            -1
        }

        val countNewAndChangedRecords = if (metrics.nonEmpty)
          metrics("countNewAndChangedRecords").asInstanceOf[Long]
        else {
          if (sourceHasPrimaryKey && !action.getIsVersioned)
            spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
              .load(action.getDestinationFilePath)
              .where(s"""${metadataCols("RowTimestamp")} >= "$loadStartTime" """)
              .count()
          else
            -1
        }

        val countOlderVersions = if (metrics.nonEmpty)
          metrics("countOlderVersions").asInstanceOf[Long]
        else {
          if (sourceHasPrimaryKey && action.getIsVersioned)
            spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
              .load(action.getDestinationFilePath)
              .where(s"""${metadataCols("EffectiveDateEnd")} < "$prevEffDateYYYY_MM_DD" """)
              .count()
          else
            -1
        }

        val countCurrentUnchangedVersions = if (metrics.nonEmpty)
          metrics("countCurrentUnchangedVersions").asInstanceOf[Long]
        else {
          if (sourceHasPrimaryKey && action.getIsVersioned)
            spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
              .load(action.getDestinationFilePath)
              .where(s"""${metadataCols("EffectiveDateStart")} < "$effDateYYYY_MM_DD" AND ${metadataCols("EffectiveDateEnd")} = "$farFutureDateYYYY_MM_DD" """)
              .count()
          else
            -1
        }

        val countDeletedVersions = if (metrics.nonEmpty)
          metrics("countDeletedVersions").asInstanceOf[Long]
        else {
          if (sourceHasPrimaryKey && action.getIsVersioned)
            spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
              .load(action.getDestinationFilePath)
              .where(s"""${metadataCols("EffectiveDateStart")} < "$effDateYYYY_MM_DD" AND ${metadataCols("EffectiveDateEnd")} = "$prevEffDateYYYY_MM_DD" """)
              .count()
          else
            -1
        }

        val countFirstVersions = if (metrics.nonEmpty)
          metrics("countFirstVersions").asInstanceOf[Long]
        else {
          if (sourceHasPrimaryKey && action.getIsVersioned)
            spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
              .load(action.getDestinationFilePath)
              .where(s"""${metadataCols("EffectiveDateStart")} = "$effDateYYYY_MM_DD" AND ${metadataCols("EffectiveDateEnd")} = "$farFutureDateYYYY_MM_DD" AND ${metadataCols("Version")} = 1 """)
              .count()
          else
            -1
        }

        val countNewVersions = if (metrics.nonEmpty)
          metrics("countNewVersions").asInstanceOf[Long]
        else {
          if (sourceHasPrimaryKey && action.getIsVersioned)
            spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
              .load(action.getDestinationFilePath)
              .where(s"""${metadataCols("EffectiveDateStart")} = "$effDateYYYY_MM_DD" AND ${metadataCols("EffectiveDateEnd")} = "$farFutureDateYYYY_MM_DD" AND ${metadataCols("Version")} > 1 """)
              .count()
          else
            -1
        }

        // Create new LoadControl record
        Seq(
          Row(
            action.getName, // "LoadName"
            action.getSourceTypeDescription, // "SourceType"
            sourceDescriptionAbbreviated, // "ProcessedSource"
            action.getDestinationTypeDescription, // "DestinationType"
            action.getDestinationDescription, // "Destination"
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
        .withColumns(ListMap( // ListMap preserves the order of columns
          "CreatedOn" -> {
            val value = current_timestamp();
            when(value.isNotNull, value).otherwise(lit(null))
          },
          "CreatedBy" -> {
            val value = lit(System.getProperty("user.name")); // identity of who runs the job
            when(value.isNotNull, value).otherwise(lit(null))
          },
          "EffectiveDate" -> {
            if (effectiveDate == null) to_date(lit(null)) else to_date(lit(getDateFormatted(effectiveDate, "yyyy-MM-dd")))
          }
        ))
        /*
                .withColumn("CreatedOn", current_timestamp())
                .withColumn("CreatedBy", lit(System.getProperty("user.name"))) // identity of who runs the job
                .withColumn("EffectiveDate", if (effectiveDate == null) to_date(lit(null)) else to_date(lit(getDateFormatted(effectiveDate, "yyyy-MM-dd"))))
        */
        .setNullableStateForAllColumns(true)

      if (action.getIsMaintainLoadControlAsParquetFile) {
        if (action.getIsDebugDwLib) {
          loadControlDf.show()
          loadControlDf.printSchema()
        }

        FileHelper.saveDataFrameAsParquetAndMoveToParentDir(loadControlDf, "loadControl", action.getLoadControlParquetFileDir)
      }
      else {
        throw new RuntimeException("""Loader ERROR: Cannot save Load Control record. Only Parquet format is currently supported. Fix configuration for this action to specify "fileLoadControl.parquet" """)
      }
    }
  }

  /**
   *
   * @param action
   * @param isInitialLoad
   * @param isFullLoad
   * @param dfNewData
   * @param dfStgOldAsOption - if not defined it is not applicable (e.g., for initial load). For non-initial laod it must be defined
   * @param dfAllMergeKeysWithMetadataColumnsAsOption
   * @param effectiveDate
   * @param appLog
   * @return
   */
  private def mergeSourceWithUniqueKey(
      action: SourceDataAction,
      isInitialLoad: Boolean,
      isFullLoad: Boolean,
      dfNewData: DataFrame,
      dfStgOldAsOption: Option[DataFrame],
      dfAllMergeKeysWithMetadataColumnsAsOption: Option[DataFrame],
      effectiveDate: Date): DataFrame = {

    val spark = SparkSession.builder().getOrCreate() // this gets previously created session

    val effDateYYYY_MM_DD = getDateFormatted(effectiveDate, "yyyy-MM-dd")
    val prevEffDateYYYY_MM_DD = getDateFormatted(effectiveDate, "yyyy-MM-dd", -1)

    appLog.info("Running for effective date " + effDateYYYY_MM_DD)

    if (isInitialLoad) {
      appLog.info("Running initial load for versioned data source with unique keys")
      // if (action.getIsFileDestination) saveNewStgFile(action, dfNewData, appLog)
      dfNewData
    }
    else {
      appLog.info("Running subsequent load for versioned data source with unique keys")
      require(dfStgOldAsOption.isDefined)
      val dfStg = dfStgOldAsOption.get

      /*
            val dfStg = if (dfOldDataAsOption.isDefined) {
              dfOldDataAsOption.get
            } else if (action.getSdaIsFileDestinationParquet) {
              spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
                .load(action.getSdaDestinationFilePath)
            }
            else
              throw new RuntimeException("""Loader ERROR: Only Parquet destination is currently supported """)
      */

      dfStg.createOrReplaceTempView("StgData") // This is the existing file that we want to amend to create a new one and replace the existing one with the new

      if (action.getIsDebugDwLib) {
        dfStg.select(date_format(min(col(metadataCols("RowTimestamp"))), "yyyyMMdd hhmmss.SSS").as("min formatted"),
          date_format(max(col(metadataCols("RowTimestamp"))), "yyyyMMdd hhmmss.SSS").as("max formatted")).show(5)
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
        require(dfAllMergeKeysWithMetadataColumnsAsOption.isDefined == true, """Loader ERROR: Incremental load requires defined dfAllMergeKeysAsOption """)
        require(action.getMergeKeysList == action.getPrimaryKeysList, """Loader ERROR: Merge and Primary keys must be the same for the incremental load""")
        dfAllMergeKeysWithMetadataColumnsAsOption
          .get
          .createOrReplaceTempView("AllMergeKeys")
      }


      /**
       * At this point we have two or three views - depending on whether it is full or incremental load -
       * that are used in the code to produce a new result:
       * "StgData" - the existing file
       * "NewData" - new data: can be complete set if full load or changes only if incremental load
       * "AllMergeKeys" - the set of merge key for incremental load
       */

      val dfStgNew = if (action.getIsVersioned) { // create versioned result
        createVersionedResultForSourceWithUniqueKey(action, isFullLoad, effDateYYYY_MM_DD, prevEffDateYYYY_MM_DD)
      }
      else { // create non-versioned result
        createNonVersionedResultForSourceWithUniqueKey(action, isFullLoad)
      }
      dfStgNew
    }

  }

  private def mergeSourceChanges(
      action: SourceDataAction,
      dfNewData: DataFrame,
      dfAllMergeKeys: DataFrame,
      dfStgOldAsOption: Option[DataFrame],
      effectiveDate: Date): DataFrame = {

    val spark = SparkSession.builder().getOrCreate() // this gets previously created session

    dfNewData
      .cache() // cache new data it is used repeatedly here
      .createOrReplaceTempView("NewData")

    val dfStg = if (dfStgOldAsOption.isDefined) {
      dfStgOldAsOption.get
    } else {
      throw new RuntimeException("""Loader ERROR: File based source to merge changes currently not supported """)
    }
    dfStg.createOrReplaceTempView("StgData") // This is the existing file that we want to amend to create a new one and replace the existing one with the new

    val mergeKeys = action.getMergeKeysList
    dfAllMergeKeys
      // even though mergeKeys cannot be emty here add the code that will create nullable column
      .withColumn(metadataCols("RowMergeKey"), if (mergeKeys.isEmpty) lit(null).cast(StringType) else concatColumns(struct(mergeKeys.head, mergeKeys.tail: _*)))
      // concatColumns(struct(mergeKeys.head, mergeKeys.tail: _*)))
      .createOrReplaceTempView("AllMergeKeys")


    // Step 1. Create un-changed data
    val sqlUnchanged
    =
      s"""
         | SELECT stg.*
         | FROM StgData AS stg
         |   INNER JOIN AllMergeKeys AS mergeKeys ON   stg.${metadataCols("RowMergeKey")} = mergeKeys.${metadataCols("RowMergeKey")}
         | WHERE NOT EXISTS (  SELECT 1
         |                     FROM NewData AS new
         |                     WHERE stg.${metadataCols("RowMergeKey")} = new.${metadataCols("RowMergeKey")} )
         |    """.stripMargin
    appLog.info("  Creating data frame with un-changed data on merge changes:\n" + sqlUnchanged)
    val dfUnchanged = spark.sql(sqlUnchanged)
    if (action.getIsDebugDwLib) {
      dfUnchanged.show(3)
    }

    // Finally merge all data frames to create a final copy
    // For changed and deleted rows on the expire rows by setting new EffectiveDateEnd
    // "EffectiveDateEnd" column must be the last one (unless dropping a column in the middle is as efficient as at the end)
    // This is the result of the "else" -- not an initial load
    val dfStgNew = dfUnchanged
      .union(dfNewData)

    if (action.getIsDebugDwLib) {
      appLog.info("Older Versions rows count: " + dfUnchanged.count())
    }

    // if (action.getIsFileDestination) saveNewStgFile(action, dfStgNew, appLog)
    dfStgNew
  }

  private def createVersionedResultForSourceWithUniqueKey(action: SourceDataAction, isFullLoad: Boolean, effDateYYYY_MM_DD: String, prevEffDateYYYY_MM_DD: String): DataFrame = {

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
      s"""|
          | SELECT *
          | FROM (
          |   SELECT ROW_NUMBER() OVER ( PARTITION BY ${metadataCols("RowUniqueKey")} ORDER BY ${metadataCols("EffectiveDateStart")} DESC ) AS ReverseVersionNumber,
          |   *
          |   FROM StgData
          |   WHERE     ${metadataCols("EffectiveDateStart")} != "$effDateYYYY_MM_DD"
          |         AND ${metadataCols("EffectiveDateEnd")} >= "$prevEffDateYYYY_MM_DD"
          | ) AS StgTableWithVersionNumber
          | WHERE ReverseVersionNumber = 1
          |   """.stripMargin
    appLog.info("  Creating data frame with most recent version excluding current effective date. It will be cached:\n" + sqlLatestVersionExcludingCurrentEffDate)
    val dfStgLatestVersionExcludingCurrentEffDate = spark.sql(sqlLatestVersionExcludingCurrentEffDate)
      .drop("ReverseVersionNumber")
    // .cache()  // caching was taken 10 extra min locally for a wide ~8MM row file on incremental load
    if (action.getIsDebugDwLib) {
      appLog.info("Most recent version excluding current effective date rows count (this is cached): " + dfStgLatestVersionExcludingCurrentEffDate.count())
    }
    dfStgLatestVersionExcludingCurrentEffDate.createOrReplaceTempView("LatestVersionExcludingCurrentEffDate") // need this as view to determine new versions
    if (action.getIsDebugDwLib) {
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
         | WHERE ${metadataCols("EffectiveDateEnd")} < "$prevEffDateYYYY_MM_DD"
         |   """.stripMargin
    appLog.info("  Creating data frame with older versions that will not change:\n" + sqlOlderVersions)
    val dfStgOlderVersions = spark.sql(sqlOlderVersions)
    if (action.getIsDebugDwLib) {
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
         |   INNER JOIN NewData AS new ON   stg.${metadataCols("RowUniqueKey")} = new.${metadataCols("RowUniqueKey")}
         |                              AND stg.${metadataCols("RowHash")} != new.${metadataCols("RowHash")}
         |    """.stripMargin
    appLog.info("  Creating data frame with latest versions that will change:\n" + sqlLatestVersionsChanged)
    val dfStgLatestVersionsChanged = spark.sql(sqlLatestVersionsChanged)
      .drop({
        metadataCols("EffectiveDateEnd")
      })
      .withColumn({
        metadataCols("EffectiveDateEnd")
      }, {
        val value = to_date(lit(prevEffDateYYYY_MM_DD));
        when(value.isNotNull, value).otherwise(lit(null))
      })
    if (action.getIsDebugDwLib) {
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
         |                     WHERE   stg.${metadataCols("RowMergeKey")} = new.${metadataCols(if (isFullLoad) "RowUniqueKey" else "RowMergeKey")} )
         |    """.stripMargin
    appLog.info("  Creating data frame with latest versions that are deleted:\n" + sqlLatestVersionsDeleted)
    val dfStgLatestVersionsDeleted = spark.sql(sqlLatestVersionsDeleted)
      .drop({
        metadataCols("EffectiveDateEnd")
      })
      .withColumn({
        metadataCols("EffectiveDateEnd")
      },
        {
          val value = to_date(lit(prevEffDateYYYY_MM_DD));
          when(value.isNotNull, value).otherwise(lit(null))
        }
      )
    if (action.getIsDebugDwLib) {
      dfStgLatestVersionsDeleted.show(3)
    }

    // On re-run some records may have EffectiveDateEnd equal to previous day. We would need to set all EffectiveDateEnd values to 2099-01-01.
    // For not changed data select from NewData because new can have columns that are excluded from difference, like counters that changed daily,
    // but we still want the latest value in the latest version of the loaded data.
    // NewData does not have version, effective date start and end. Add them from existing data.
    val sqlLatestVersionsNotChanged = if (isFullLoad) {
      s"""
         | SELECT new.*, stg.${metadataCols("Version")}, stg.${metadataCols("EffectiveDateStart")}, stg.${metadataCols("EffectiveDateEnd")}
         | FROM LatestVersionExcludingCurrentEffDate AS stg
         |   INNER JOIN NewData AS new ON   stg.${metadataCols("RowUniqueKey")} = new.${metadataCols("RowUniqueKey")}
         |                              AND stg.${metadataCols("RowHash")} = new.${metadataCols("RowHash")}
         |    """.stripMargin
    }
    else { // on incremental load NewData only has changes and new rows. Additionaly it can have chaged columns that are excluded from versioning, like counters that changed daily.
      // We still want that data with new values for columns that are excluded from versioning.
      s"""
         | SELECT stg.*
         | FROM LatestVersionExcludingCurrentEffDate AS stg
         |   INNER JOIN AllMergeKeys AS keys ON stg.${metadataCols("RowMergeKey")} = keys.${metadataCols("RowMergeKey")}
         | WHERE NOT EXISTS (SELECT 1
         |                     FROM NewData AS new
         |                     WHERE stg.${metadataCols("RowMergeKey")} = new.${metadataCols("RowMergeKey")} )
         | UNION ALL
         | SELECT new.*, stg.${metadataCols("Version")}, stg.${metadataCols("EffectiveDateStart")}, stg.${metadataCols("EffectiveDateEnd")}
         | FROM LatestVersionExcludingCurrentEffDate AS stg
         |   INNER JOIN NewData AS new ON   stg.${metadataCols("RowUniqueKey")} = new.${metadataCols("RowUniqueKey")}
         |                              AND stg.${metadataCols("RowHash")} = new.${metadataCols("RowHash")}
         |   """.stripMargin
    }

    appLog.info("  Creating data frame with latest versions that will not change:\n" + sqlLatestVersionsNotChanged)
    val dfStgLatestVersionsNotChanged = spark.sql(sqlLatestVersionsNotChanged)
      .drop(metadataCols("EffectiveDateEnd"))
      .withColumn(metadataCols("EffectiveDateEnd"),
        {
          val value = to_date(lit(farFutureDateYYYY_MM_DD));
          when(value.isNotNull, value).otherwise(lit(null))
        }
      )
    if (action.getIsDebugDwLib) {
      dfStgLatestVersionsNotChanged.show(3)
    }

    // Now include new unique key and new versions for the existing ones
    // new data is still missing three columns - Version , start and end date
    val sqlNewVersions
    =
      s"""
         | SELECT new.* ,
         |          CASE WHEN 1 = 1 THEN 1 ELSE CAST(NULL AS INT) END AS ${metadataCols("Version")},   -- set version to 1 for rows with new unique key
         |          CASE WHEN 1 = 1 THEN CAST( '$effDateYYYY_MM_DD' AS DATE ) ELSE CAST(NULL AS DATE) END AS ${metadataCols("EffectiveDateStart")},
         |          CASE WHEN 1 = 1 THEN CAST( '$farFutureDateYYYY_MM_DD' AS DATE ) ELSE CAST(NULL AS DATE) END AS ${metadataCols("EffectiveDateEnd")}
         | FROM NewData AS new
         | WHERE NOT EXISTS (  SELECT 1
         |                     FROM LatestVersionExcludingCurrentEffDate AS notChanged
         |                     WHERE notChanged.${metadataCols("RowUniqueKey")} = new.${metadataCols("RowUniqueKey")} )
         | UNION ALL
         | SELECT new.*,
         |          stg.${metadataCols("Version")} + 1 AS ${metadataCols("Version")}, -- for new versions of existing rows bump the version by 1. Also the column is already nullable so do not need additional CAST( NULL AS INT)
         |          CASE WHEN 1 = 1 THEN CAST( '$effDateYYYY_MM_DD' AS DATE ) ELSE CAST(NULL AS DATE) END AS ${metadataCols("EffectiveDateStart")},
         |          CASE WHEN 1 = 1 THEN CAST( '$farFutureDateYYYY_MM_DD' AS DATE ) ELSE CAST(NULL AS DATE) END AS ${metadataCols("EffectiveDateEnd")}
         | FROM LatestVersionExcludingCurrentEffDate AS stg
         |    INNER JOIN NewData AS new ON   stg.${metadataCols("RowUniqueKey")} = new.${metadataCols("RowUniqueKey")}
         |                               AND stg.${metadataCols("RowHash")} != new.${metadataCols("RowHash")}
         |    """.stripMargin
    val dfNewVersions = spark.sql(sqlNewVersions)
    /*      .withColumns(ListMap( // ListMap preserves the order of columns
            metadataCols("EffectiveDateStart") -> {
              val value = to_date(lit(effDateYYYY_MM_DD));
              when(value.isNotNull, value).otherwise(lit(null))
            },
            metadataCols("EffectiveDateEnd") -> {
              val value = to_date(lit(farFutureDateYYYY_MM_DD)); // This column must be the last one. It will be replaced during versioning process
              when(value.isNotNull, value).otherwise(lit(null))
            }
          ))*/

    /*
          .withColumn(metadataCols("EffectiveDateStart"), to_date(lit(effDateYYYY_MM_DD)))
          .withColumn(metadataCols("EffectiveDateEnd"), to_date(lit(farFutureDateYYYY_MM_DD))) // This column must be the last one. It will be replaced during versioning process
    */

    if (action.getIsDebugDwLib) {
      dfNewVersions.show(3)
      dfNewVersions.printSchema()
      dfStgOlderVersions.printSchema()
    }

    // Finally merge all data frames to create a final copy
    // For changed and deleted rows - expire them by setting new EffectiveDateEnd
    // "EffectiveDateEnd" column must be the last one (unless dropping a column in the middle is as efficient as at the end)
    // This is the result of the "else" -- not an initial load
    val dfStgNew = dfStgOlderVersions
      .union(dfStgLatestVersionsChanged)
      .union(dfStgLatestVersionsDeleted)
      .union(dfStgLatestVersionsNotChanged)
      .union(dfNewVersions)

    if (action.getIsDebugDwLib) {
      appLog.info("Older Versions rows count: " + dfStgOlderVersions.count())
      appLog.info("Latest Versions Changed rows count: " + dfStgLatestVersionsChanged.count())
      appLog.info("Latest Versions Deleted rows count: " + dfStgLatestVersionsDeleted.count())
      appLog.info("Latest Versions Not Change rows count: " + dfStgLatestVersionsNotChanged.count())
      appLog.info("New versions rows count: " + dfNewVersions.count())
    }

    // if (action.getIsFileDestination) saveNewStgFile(action, dfStgNew, appLog)
    dfStgNew
  }

  private def createNonVersionedResultForSourceWithUniqueKey(action: SourceDataAction, isFullLoad: Boolean): DataFrame = {

    val spark = SparkSession.builder().getOrCreate() // this gets previously created session

    // Step 1. Create changed data
    val sqlChanged
    =
      s"""
         | SELECT stg.*
         | FROM StgData AS stg
         |   INNER JOIN NewData AS new ON   stg.${metadataCols("RowUniqueKey")} = new.${metadataCols("RowUniqueKey")}
         |                              AND stg.${metadataCols("RowHash")} != new.${metadataCols("RowHash")}
         |    """.stripMargin
    appLog.info("  Creating data frame with changed data:\n" + sqlChanged)
    val dfChanged = spark.sql(sqlChanged)
    if (action.getIsDebugDwLib) {
      dfChanged.show(3)
    }

    // Step 2. Create not changed data excluding deleted records
    val sqlNotChangedExcludingDeleted = if (isFullLoad) {
      s"""
         | SELECT stg.*
         | FROM StgData AS stg
         |   INNER JOIN NewData AS new ON   stg.${metadataCols("RowUniqueKey")} = new.${metadataCols("RowUniqueKey")}
         |                              AND stg.${metadataCols("RowHash")} = new.${metadataCols("RowHash")}
         |
          """.stripMargin
    }
    else { // Incremental load
      s"""
         | SELECT stg.*
         | FROM StgData AS stg
         |   INNER JOIN AllMergeKeys AS keys ON stg.${metadataCols("RowMergeKey")} = keys.${metadataCols("RowMergeKey")}
         | WHERE NOT EXISTS (SELECT 1
         |                     FROM NewData AS new
         |                     WHERE stg.${metadataCols("RowMergeKey")} = new.${metadataCols("RowMergeKey")} )
          """.stripMargin
    }

    appLog.info("  Creating data frame with not changed records:\n" + sqlNotChangedExcludingDeleted)
    val dfStgNotChangedExcludingDeleted = spark.sql(sqlNotChangedExcludingDeleted)
    if (action.getIsDebugDwLib) {
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
         |                     WHERE stg.${metadataCols("RowUniqueKey")} = new.${metadataCols("RowUniqueKey")} ) """.stripMargin
    val dfNew = spark.sql(sqlNew)

    if (action.getIsDebugDwLib) {
      dfNew.show(3)
    }

    // Finally merge all data frames to create a final copy
    // For changed and deleted rows on the expire rows by setting new EffectiveDateEnd
    // "EffectiveDateEnd" column must be the last one (unless dropping a column in the middle is as efficient as at the end)
    // This is the result of the "else" -- not an initial load
    val dfStgNew = dfChanged
      .union(dfStgNotChangedExcludingDeleted)
      .union(dfNew)

    if (action.getIsDebugDwLib) {
      appLog.info("Older Versions rows count: " + dfChanged.count())
      appLog.info("Latest Versions Not Change rows count: " + dfStgNotChangedExcludingDeleted.count())
      appLog.info("New versions rows count: " + dfNew.count())
    }

    // if (action.getIsFileDestination) saveNewStgFile(action, dfStgNew, appLog)
    dfStgNew
  }

  private def mergeSourceWithNoUniqueKey(action: SourceDataAction, isInitialLoad: Boolean, dfNewData: DataFrame, dfStgOldAsOption: Option[DataFrame], effectiveDate: Date): DataFrame = {

    val spark = SparkSession.builder().getOrCreate() // this gets previously created session

    val effDateYYYY_MM_DD = getDateFormatted(effectiveDate, "yyyy-MM-dd")

    appLog.info("Running for effective date " + effDateYYYY_MM_DD)

    val dfResult = if (!action.getIsVersioned) {
      appLog.info("Running load for non-versioned data source with no unique keys")
      // if (action.getIsFileDestination) saveNewStgFile(action, dfNewData, appLog) // Just create anew file by overwriting the existing one
      dfNewData
    }
    else if (isInitialLoad) { // i.e., versioned and initial load
      appLog.info("Running initial load for versioned data source with no unique keys")
      // if (action.getIsFileDestination) saveNewStgFile(action, dfNewData, appLog)
      dfNewData
    }
    else { // i.e., versioned and subsequent load
      appLog.info("Running subsequent load")
      require(dfStgOldAsOption.isDefined)
      val dfStg = dfStgOldAsOption.get
      if (action.getIsDebugDwLib) {
        appLog.info("Stg count: " + dfStg.count())
      }

      // The goal  here is to create new file with needed changes and all rows that are valid
      // (as opposed to table based approach where you modify the existing table to have valid rows)
      dfStg.createOrReplaceTempView("StgData") // This is the existing file that we want to amend to create a new one and replace the existing one with the new

      val sqlExcludingCurrentEffDate
      =
        s"""
           | SELECT *
           | FROM StgData
           | WHERE ${metadataCols("EffectiveDate")} != "$effDateYYYY_MM_DD"
           |   """.stripMargin
      appLog.info("  Creating data frame with data excluding current effective date:\n" + sqlExcludingCurrentEffDate)
      val dfStgExcludingCurrentEffDate = spark.sql(sqlExcludingCurrentEffDate)

      if (action.getIsDebugDwLib) {
        appLog.info("Staging data excluding current effective date rows count (this is cached): " + dfStgExcludingCurrentEffDate.count())
        dfStgExcludingCurrentEffDate.show(3)
      }


      // Finally merge all data frames to create a final copy
      // For changed and deleted rows on the expire rows by setting new EffectiveDateEnd
      // "EffectiveDateEnd" column must be the last one (unless dropping a column in the middle is as efficient as at the end)
      // This is the result of the "else" -- not an initial load
      val dfStgNew = dfStgExcludingCurrentEffDate
        .union(dfNewData)

      if (action.getIsDebugDwLib) {
        appLog.info("Staging excluding current effective date rows count: " + dfNewData.count())
        appLog.info("New data rows count: " + dfNewData.count())
      }

      // if (action.getIsFileDestination) saveNewStgFile(action, dfStgNew, appLog)
      dfStgNew
    }
    dfResult
  }

  protected def saveMergedFile(action: SourceDataAction, dfStgNew: DataFrame): Unit = {
    // Before saving the file set the metadata columns to nullable.
    // Drill and other parquet readers are complaining
    val dfStgNewWithNullableMetadataCols = dfStgNew.setNullableStateForAllColumns(true)

    if (!action.getIsFileDestinationParquet) {
      throw new RuntimeException("""Loader ERROR: Only Parquet destination is currently supported """)
    }

    if (action.getIsDebugDwLib) {
      dfStgNewWithNullableMetadataCols.printSchema()
      appLog.info("Saved new version of the staging file with row count:" + dfStgNew.count())
    }

    if (action.getIsSavePreviousVersionOfDestinationFile) {
      FileHelper.saveDataFrameAsParquet(dfStgNewWithNullableMetadataCols, action.getDestinationFilePath, action.getFileDestinationSavePreviousVersionAs)
    }
    else {
      FileHelper.saveDataFrameAsParquet(dfStgNewWithNullableMetadataCols, action.getDestinationFilePath)
    }
  }

}
