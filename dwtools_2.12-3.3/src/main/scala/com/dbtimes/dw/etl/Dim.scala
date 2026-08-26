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
package com.dbtimes.dw.etl

import java.util.Date
// import scala.util.chaining._			// This is available in scala 2.13 
import org.apache.spark.storage.StorageLevel

import java.sql.{Date => SqlDate, Timestamp => SqlTimestamp}
import java.util.{Calendar, Date}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import com.dbtimes.dw.common.DataFrameHelper._
import com.dbtimes.dw.common.{DataFrameHelper, FileHelper}
import com.dbtimes.dw.common.MiscHelper.getDateFormatted
import com.dbtimes.dw.common.LogFile.{dwlogger => dwEtlLog}
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER

import scala.collection.immutable.ListMap

/** This class implements the common logic to maintain a dimension. It handles
 *    - etl fields
 *    - updating dimension type 1 and type 2 columns
 *    - initial and incremental load
 *
 * From Spark performance standpoint the couple of of ideas implemented here with regard to setting
 * surrogate keys which is the most un-Spark operation in the whole ETL
 *    - splitting type one and type two changes into separate stages
 *      That allows to make changes to one or the other in case the dimension has only type one or
 *      only type two columns.
 *    - Type one are split into three groups
 *      * Group-1-old-changed: The old (or existing) rows whose type 1 columns changed. Surrogate key is set for these
 *      * Group-1-old-unchanged: The old (or existing) rows whose type 1 columns are not changed. Surrogate key is set for these
 *      * Group-1-new-compound-key: new rows. Surrogate key is not set
 *
 * This split allows to use Group-1-old-changed and Group-1-new to set type one changes to
 * respectively old and new groups of type 2 changes. It also allows to set surrogate keys to Group-1-new only.
 *
 *    - Type two are split into three groups
 *      * Group-2-existing-compound-with-surrogate-key. These are rows with existing compound key whose surrogate key has not chaged
 *      * Group-2-existing-compound-with-no-surrogate-key. These are new versions of the compound key with surrogate key not set
 *      * Group-2-new-compound-key.
 *
 * Type one changes can be applied only from Group-1-old-changed to Group-2-existing-compound-with-surrogate-key and
 * Group-2-existing-compound-with-no-surrogate-key, and from Group-1-new-compound-key to Group-2-new-compound-key.
 *
 * The surrogate key would only need to be set to Group-1-newcompoundd-key, Group-2-existing-compound-with-no-surrogate-key and
 * Group-2-new-compound-key
 *
 *    - When processing multiple effective dates during initial or incremental load, all the dates are processed in one shot.
 *      The alternative (DBMS way) to process one day at a time did not work here as two many dataframes were produced.
 *
 */

private[etl] object Dim {
  private var dimAuthorityPackageName = ""

  private[etl] def setDimAuthorityPackageName(dimAuthorityPackageName: String): Unit = {
    Dim.dimAuthorityPackageName = dimAuthorityPackageName
  }

  private[etl] def getDim(dimName: String): Dim = {

    val constructor = Class.forName(dimAuthorityPackageName + "." + dimName).getDeclaredConstructor(classOf[String])
    val dim = constructor.newInstance(dimName).asInstanceOf[Dim]
    dim
  }

  /**
   * On initial load delete existing dimensions or save them as previous version
   * That will allow referencing previous version as staging source
   *
   * @param configDwEtl
   * @return
   */
  private[etl] def prepareForInitialLoad(configDwEtl: ConfigDwEtl): Unit = {
    configDwEtl.getDimNamesToLoad.foreach { dimName =>
      val dimFilePath = configDwEtl.getDimensionFilePath(dimName)
      if (configDwEtl.getIsInitialLoad) {
        if (configDwEtl.getIsSavePreviousDimVersionOfDestinationFile) {
          FileHelper.moveDirectory(dimFilePath, configDwEtl.getDimensionPreviousVersionFilePath(dimName))
        }
        else {
          FileHelper.deleteDirectoryOrFileIfExists(dimFilePath)
        }
      }
    }
  }

  /**
   *
   * @param configDwEtl
   * @return - a map of dimNme -> dfDim
   *         This map will only have dimensions that are processed.
   *         The dimension will not be processed if if it is
   *         isLoad is false in config file or if there is
   *         nothing to do for dimension on incremental load.
   *         This map can therefore be empty.
   */
  private[etl] def etlAllDimensions(configDwEtl: ConfigDwEtl): Map[String, DataFrame] = {

    // Each previously loaded dimension can be used to load next dimension
    // To allow for such logic accumulate DataFrames of previously loaded dimensions in the list that will be passed to loading
    // each next dimension.
    // It is a responsibility of a configuration to arrange the dimensions in the order that would load the
    // dimensions used to load other dimensions earlier.
    // On incremental load it is possible that a dimension used to load some other dimension is returned as None
    // (there are no changes to the dimension). In that case the dimension will be loaded from the file
    // (similar to the logic of loading dimensions for facts to set keys or load facts from dimension).

    // loop through all configurations and prepare a list of dimensions for load
    val dimNamesAndDfPlaceholder: List[(String, Map[String, DataFrame])] = for (
      dimName <- configDwEtl.getDimNamesToLoad;
      dimsAndDfs: Map[String, DataFrame] = Map()  // The type of map here is for documentation
    ) yield (dimName, dimsAndDfs)

    // Start value has to be of the same type as elements of the List for fold.
    val startValue = (
      "",
      Map[String, DataFrame]()
    )

    // In the very first iteration the curr is the first element of the list - curr - will be used together with the start value - prev.
    // The second iteration of the fold left, it will use the previous result, where the last tuple element will have the fact table with the first key.
    val (_, mapOfLoadedDimensionsWithNonEmptyResult) = dimNamesAndDfPlaceholder
      .foldLeft(startValue)((prev, curr) => {
        val (_, mapOfPreviouslyLoadedDimensionsWithNonEmptyResult) = prev
        val (dimName, _) = curr
        (dimName, // this is the dimension to be processed in this iteration - we don't really need it in return value (we only need a map that may be used to process next dimension),
          // but it does not hurt to have it.
          { //
            Dim.defineViewsForDimDimSources(dimName, mapOfPreviouslyLoadedDimensionsWithNonEmptyResult, configDwEtl)
            val dfDimAsOption = Dim.etlDimension(dimName, configDwEtl)
            // Add new df to the map of loaded dimension if new df is non-empty
            mapOfPreviouslyLoadedDimensionsWithNonEmptyResult ++ (if (dfDimAsOption.isDefined) Map(dimName -> dfDimAsOption.get) else Nil)
          })
      }
      )

    mapOfLoadedDimensionsWithNonEmptyResult // this is the accumulated value for all loaded dimensions
  }

  /**
   * Make dimensions listed as sources for loading a dimension available for loading that dimension.
   * These source dimensions will be available as views to be referenced in the ETL override.
   * They will not be used to create callbacks similar to regular staging sources
   *
   * @param dimName
   * @param dimNamesDfsOfPreviouslyLoadedDimensions - means dimensions previously loaded in this
   *                                                ETL process as opposed to dimension loaded before in a different ETL process.
   * @param configDwEtl
   * @return
   */
  private def defineViewsForDimDimSources(dimName: String, dimNamesDfsOfPreviouslyLoadedDimensions: Map[String, DataFrame], configDwEtl: ConfigDwEtl): Unit = {
    dwEtlLog.info("---- Defining views for dimension sources for dimension: " + dimName)
    val spark = SparkSession.builder().getOrCreate() // this gets previously created session

    // get dimensions used to load this dimension
    val dimDimSourceNames = configDwEtl.getDimSourceNamesOfDim(dimName)

    // loop through all sources and load dimensions that not already loaded
    dimDimSourceNames.foreach(dimDimSourceName => {
      val dimDimSourceDf = if (dimNamesDfsOfPreviouslyLoadedDimensions.keys.exists(_ == dimDimSourceName)) {
        dimNamesDfsOfPreviouslyLoadedDimensions(dimDimSourceName)
      }
      else {
        spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
          .load(configDwEtl.getDimensionFilePath(dimDimSourceName))
      }

      val colsInDimSourceToLoadDimension = Dim.getDimensionColsForLoadingDimension(dimName, dimDimSourceName, configDwEtl)
      val dimDimSourcedWithNeededColumns = dimDimSourceDf.select(colsInDimSourceToLoadDimension.head, colsInDimSourceToLoadDimension.tail: _*)
      dimDimSourcedWithNeededColumns.createOrReplaceTempView(dimDimSourceName) // Now dimension is available as view in dimension ETL. The same dim source will have same name but may have different columns for different dimensions that uses it in loading itself
    }
    )
  }

  private def etlDimension(dimName: String, configDwEtl: ConfigDwEtl): Option[DataFrame] = {
    dwEtlLog.info("---- Processing dimension: " + dimName)

    val timeStart = Calendar.getInstance().getTimeInMillis()
    val dim = Dim.getDim(dimName)
    val stgSrcViewsWithMonikerNames: List[String] = dim.loadStagingSources()
    val datesToProcess: List[Date] = dim.getEffectiveDatesToProcess()

    val dfDimAsOption = if (datesToProcess.isEmpty) {
      dwEtlLog.info(s"-- There are no dates to process dimension $dimName")
      None
    } else {
      dim.etlModelObject(stgSrcViewsWithMonikerNames, datesToProcess)
    }

    val durationSec = (Calendar.getInstance().getTimeInMillis() - timeStart).toDouble / 1e3 // seconds

    // For the Etl Process record the last effective date from the shared dates to process because that is
    // created from the sources marked for effective date and that's what will be used to get a new
    // dates range for the incremental load
    ModelObject.createEtlLogRecord(dimName, configDwEtl, configDwEtl.getStgSourceMonikersOfDim(dimName), datesToProcess,
      configDwEtl.getIsInitialLoad, if (configDwEtl.getRerunEtlAfter.isDefined) true else false, durationSec)

    dfDimAsOption
  }

  /**
   *
   * @param dimName - this dimension can either be used to set fact table keys or to load the fact table
   * @param configDwEtl
   * @return
   */
  private[etl] def getDimensionColsToSetKeyOrLoadingFact(dimName: String, configDwEtl: ConfigDwEtl): List[String] = {
    val colsInFactSchemasToSetKey = configDwEtl.getDimensionColsToSetKeyForAllFacts(dimName)

    // The dimension can also be used as a source to load some fact tables.
    // Add columns used to load all fact tables. These columns may be empty list or it could be all columns from the dimension
    val colsInFactSchemasToLoadFactTables = configDwEtl.getDimensionColsUsedInLoadingAllFacts(dimName)

    // Combine both lists and drop columns that are added below in case they were listed in the configuration
//    val colsInFactSchemasToSetKeyOrToLoadFactTables = colsInFactSchemasToSetKey.concat(colsInFactSchemasToLoadFactTables) // Scala 2.13 replaced union with concat as distinct that follows gets rid of duplicates
    val colsInFactSchemasToSetKeyOrToLoadFactTables = colsInFactSchemasToSetKey.union(colsInFactSchemasToLoadFactTables) // Refactored for Scala 2.12 - back to union
      .distinct
      .filterNot(_ == configDwEtl.getDimensionKeyCol(dimName))
      .filterNot(_ == "CompoundKey")
      .filterNot(_ == "StartDate")
      .filterNot(_ == "EndDate")

    // In addition to these columns add the key itself, Compound key, and for type 2 dimension, add start and end dates
    val withAdditionalCols = configDwEtl.getDimensionKeyCol(dimName) :: "CompoundKey" :: colsInFactSchemasToSetKeyOrToLoadFactTables

    if (configDwEtl.getDimTypeTwoCols(dimName).isEmpty) {
      withAdditionalCols
    }
    else {
      "StartDate" :: "EndDate" :: withAdditionalCols
    }
  }

  /**
   * Iterate over all fact tables to be loaded and select all columns from this dimension used to load fact table
   *
   * @param dimName
   * @param dimDimSourceName - dimension that is the source of loading dimName
   * @param configDwEtl
   * @return - the list of all columns that are used to load dimension.
   */
  private[etl] def getDimensionColsForLoadingDimension(dimName: String, dimDimSourceName: String, configDwEtl: ConfigDwEtl): List[String] = {

    val columnsInConfiguration = configDwEtl.getDimensionColsUsedInLoadingDimension(dimName, dimDimSourceName);

    val columnsInConfigurationMinusColumnsToBeAdded = columnsInConfiguration
      .distinct // just in case there are duplicates in config
      .filterNot(_ == configDwEtl.getDimensionKeyCol(dimDimSourceName)) // remove columns to be added below
      .filterNot(_ == "CompoundKey")
      .filterNot(_ == "StartDate")
      .filterNot(_ == "EndDate")

    // In addition to these columns add the key itself, Compound key, and for type 2 dimension, add start and end dates
    val withAdditionalCols = configDwEtl.getDimensionKeyCol(dimDimSourceName) :: "CompoundKey" :: columnsInConfigurationMinusColumnsToBeAdded

    val withMoreAdditionalCols = if (configDwEtl.getDimTypeTwoCols(dimDimSourceName).isEmpty) {
      withAdditionalCols
    }
    else {
      "StartDate" :: "EndDate" :: withAdditionalCols
    }

    withMoreAdditionalCols
  }

}

/**
 * On incremental load when determining the dates to load - determine two items last loaded effective date and the timestamp of the last loaded dimension row
 *
 * @param dimName - dimName is a class parameter and a field with the same name because it has val before the name
 */
//noinspection ScalaStyle

abstract class Dim(private val dimName: String)
  extends ModelObject(dimName) {

  private val farFutureDateYYYY_MM_DD = "2099-01-01"
  private val farPastDateYYYY_MM_DD = "1900-01-01"
  private var dimColsInclEtlOnes: Array[String] = Array.empty

  // Dimension specific views to have dimension name as part of it so not to collide with the views for different dimensions
  private val viewDimFromStgSources = dimName + "FromStgSources"
  private val viewDimExisting = dimName + "Existing"

  protected def dimKey: String = configDwEtl.getDimensionKeyCol(dimName) // needed to implement custom setForeignKeyOnFactTable
  protected def dimKeyUnknownValue: String = configDwEtl.getDimensionColNamesAndUnknownValues(dimName)(dimKey)

  private val dimSchema = configDwEtl.getDimensionSchema(dimName)
  private val dimSchemaWithoutKey = configDwEtl.getDimensionSchemaWithoutKey(dimName)
  private val dimCols = configDwEtl.getDimCols(dimName)
  private val naturalKeys = configDwEtl.getDimNaturalKeys(dimName)
  private val typeZeroCols = configDwEtl.getDimTypeZeroCols(dimName)
  private val typeOneCols = configDwEtl.getDimTypeOneCols(dimName)
  private val typeTwoCols = configDwEtl.getDimTypeTwoCols(dimName)
  private val typeThreeCols = configDwEtl.getDimTypeThreeCols(dimName)
  private val hasTypeZeroCols = !typeZeroCols.isEmpty
  private val hasTypeOneCols = !typeOneCols.isEmpty
  private val hasTypeTwoCols = !typeTwoCols.isEmpty
  private val hasTypeThreeCols = !typeThreeCols.isEmpty
  private val hasTypeZeroOrThreeCols = hasTypeZeroCols || hasTypeThreeCols

  // Types zero and three are similar to type one in a sense that the values are updated for a given field on all versions
  // of a row for a given CompoundKey, if there are type two columns.
  // Types zero and three are different from type one in turms of processing as they cannot be handled in bulk using hash
  // because the logic to update or keep is based on the values in new or existing data.
  // We will process types zero and three together with type one, but will use separate logic.

  // At least one column is of type one, zero or three or all columns are natural keys
  private val hasTypeZeroOneThreeColsOrNaturalKeysOnly = (hasTypeOneCols || hasTypeZeroCols || hasTypeThreeCols || (!hasTypeOneCols && !hasTypeTwoCols && !hasTypeZeroCols && !hasTypeThreeCols))

  // Create empty dataframe with the correct schema, if the source does not need to be processed
  // or when the returning the empty dataset for default load dim implementations
  private val dfEmptyStgSrcDim: DataFrame =
    sparkModelObject.createDataFrame(sparkModelObject.sparkContext.emptyRDD[Row], dimSchema)

  /**
   * Override these methods in dimension class to load/enrich source data
   *
   * Notes: this is the code snippet to get last loaded timestamp in the loadDim if needed.
   * The second line would convert it to string if needed to be used inside SQL statement
   * val stgSrcLastLoadedTimestampAsOption = lastProcessedStgSourceTimestamp.getOrElse( stgSrcViewWithMonikerName, None );
   * val stgSrcLastLoadedTimestampAsStringAsOption = if ( stgSrcLastLoadedTimestampAsOption.isDefined ) Some( stgSrcLastLoadedTimestampAsOption.get.toString ) else None;
   */
  protected def loadDim(stgSrcViewWithMonikerName: String): Option[DataFrame] = None // Override this method to load new data in bulk or for all effective dates

  protected def loadDim(effDateYYYY_MM_DD: String, stgSrcViewWithMonikerName: String): Option[DataFrame] = None // Override this method to load new data one effective dates at a time. Dimensions with type 2 columns can only use this method to load new data

  // dimExistingTempViewAsOption will be set to None for initial load
  protected def enrichDim(dfSrcDim: DataFrame, dimExistingTempViewAsOption: Option[String]): DataFrame = {
    dfSrcDim
  }

  final override private[etl] def etlModelObject(stgSrcViewsWithMonikerNames: List[String], datesToProcess: List[Date]): Option[DataFrame] = {

    dwEtlLog.info(s"-- Processing dimension $dimName for following ${datesToProcess.size} dates: ${datesToProcess.mkString(",")}")

    val dfDim = getOrCreateDimension()
    if (!dimColsInclEtlOnes.isEmpty) throw new IllegalStateException("dimColsInclEtlOnes Already initialized")
    dimColsInclEtlOnes = dfDim.schema.fieldNames

    preProcess(stgSrcViewsWithMonikerNames, datesToProcess)

    if (isDebugDwLib) {
      dfDim.printSchema()
    }

    //    Another way to do it
    //    val nextKey = dfDim.select( max( dimKey).cast(LongType)).collect()(0).getLong(0) + 1L
    val maxKeyAndStartDate: Row = dfDim.agg(max(dimKey).cast(LongType), max("StartDate")).head
    val nextKey = maxKeyAndStartDate.getLong(0) + 1L
    val lastStartDate = maxKeyAndStartDate.getDate(1)

    if (isDebugDwLib) {
      dfDim.show(5);
      dfDim.printSchema()
    }

    // Delete all effective dates after the earliest effective date. Will be used on re-runs
    // Only do it for type two dimension. For other, we do not care if we have some unreferenced dimension members
    // This also means that type two dimension will reload the previously loaded source on loading a new source for the same day
    val dfRemovedFutureEffectiveDates = if (!isInitialLoad && hasTypeTwoCols && lastStartDate.compareTo(datesToProcess.head) >= 0) {
      removeCurrentAndFutureEffectiveDates(dfDim, datesToProcess.head)
    } else {
      dfDim
    }

    if (isDebugDwLib) {
      // dfPlayByPlay.show( 5);
      dfRemovedFutureEffectiveDates.show(5);
      dfRemovedFutureEffectiveDates.printSchema()
    }

    // Try to load dimension for all effective dates at once
    // Effective date is not passed, will try to load for all effective dates
    val dfDimForAllEffectiveDatesAtOnceAsOption = doLoadForAllSources(stgSrcViewsWithMonikerNames)

    val dfDimForAllEffectiveDatesOneAtATimeAsOption = if (!dfDimForAllEffectiveDatesAtOnceAsOption.isDefined) {
      // The value of this Option can be None if none of the sources were used to extract dimension
      // This can happen, for example, when the source is loaded only when changed and none of the sources changed
      // on subsequent load

      // Get results from each effective date.
      // Here in the for loop we create a df for each effective date and
      // merge all these dfs into a single df
      val startValue = (
        new Date(0L): Date,
        None: Option[DataFrame]
      )

      val datesToProcessAndDataFramePlaceholder = for (effDate <- datesToProcess) yield (effDate, None: Option[DataFrame])

      val (_, dfDimForAllEffectiveDatesAsOption) = datesToProcessAndDataFramePlaceholder
        .foldLeft(startValue)((prev, curr) => {
          val (_, dfDimSoFarAsOption) = prev
          val (effDate, _) = curr
          (
            effDate, {
            val dfDimForEffectiveDateAsOption = doLoadForAllSources(stgSrcViewsWithMonikerNames, getDateFormatted(effDate, "yyyy-MM-dd"))

            if (dfDimSoFarAsOption.isDefined && dfDimForEffectiveDateAsOption.isDefined) {
              Some(dfDimSoFarAsOption.get.union(dfDimForEffectiveDateAsOption.get))
            }
            else if (dfDimSoFarAsOption.isDefined && !dfDimForEffectiveDateAsOption.isDefined) {
              dfDimSoFarAsOption
            }
            else if (!dfDimSoFarAsOption.isDefined && dfDimForEffectiveDateAsOption.isDefined) {
              dfDimForEffectiveDateAsOption
            }
            else { // empty data frame
              None
            }
          }
          )
        }
        )

      /*
            val dfDimForEachEffectiveDateAsOption = for (effDate <- datesToProcess;
                                                         dfDimForEffectiveDateAsOption = doLoadForAllSources(stgSrcViewsWithMonikerNames, getDateFormatted(effDate, "yyyy-MM-dd")) // effDate.toString
                                                         ) yield dfDimForEffectiveDateAsOption
            // merge all dataframes into one for all effective dates
            val dfDimForAllEffectiveDatesAsOption = dfDimForEachEffectiveDateAsOption.reduceLeft((df1AsOption, df2AsOption) =>
              if (df1AsOption.isDefined && df2AsOption.isDefined) {
                Some(df1AsOption.get.union(df2AsOption.get))
              }
              else if (df1AsOption.isDefined && !df2AsOption.isDefined) {
                df1AsOption
              }
              else if (!df1AsOption.isDefined && df2AsOption.isDefined) {
                df2AsOption
              }
              else { // empty data frame
                None
              }
            )
      */

      dfDimForAllEffectiveDatesAsOption
    }
    else {
      None
    }

    val dfDimProcessedAsOption = if (dfDimForAllEffectiveDatesAtOnceAsOption.isDefined || dfDimForAllEffectiveDatesOneAtATimeAsOption.isDefined) {

      // This is the result from all sources
      // Also exclude a row with natural key(s) the same as for Unknown row. It is already part of a dimension.
      val dfDimForAllEffectiveDates = dfDimForAllEffectiveDatesAtOnceAsOption.getOrElse(dfDimForAllEffectiveDatesOneAtATimeAsOption.get)

      if (isDebugDwLib) {
        dfDimForAllEffectiveDates.show(10)
      }

      // There was an attempt to add etl columns here after all data is collected. It will not work for the case
      // When each effective date is loaded separately or from different sources since ETL columns have sourceSystem and effective date columns

      // build new dimension
      val dfRemovedFutureEffectiveDatesCached = dfRemovedFutureEffectiveDates.cache() // cache does not seem to matter on initial load
      val dfDimForAllEffectiveDatesPersisted = dfDimForAllEffectiveDates.persist(StorageLevel.MEMORY_AND_DISK) // persist does not seem to matter on initial load
      val (dfDimWithExistingSurrogKey, dfDimNewWithSurrogKeyNull) = createNewDimVersion(dfRemovedFutureEffectiveDatesCached, dfDimForAllEffectiveDatesPersisted)

      // Generate surrogate keys if needed
      val dfDimResult = if (configDwEtl.getIsDimensionKeySurrogate(dimName)) {
        val dfWithNewSurrogateKeySet = setSurrogateKeyValue(dfDimNewWithSurrogKeyNull, nextKey)

        if (isDebugDwLib) {
          dfWithNewSurrogateKeySet.show(40)
          dfDimWithExistingSurrogKey.show(40)
        }

        dfDimWithExistingSurrogKey.union(dfWithNewSurrogateKeySet)
      }
      else {
        dfDimWithExistingSurrogKey.union(dfDimNewWithSurrogKeyNull)
      }

      // Post process and save dimension
      val dfDimPostProcessed = postProcess(stgSrcViewsWithMonikerNames, datesToProcess, dfDimResult)
      saveDim(dfDimPostProcessed)
      Some(dfDimPostProcessed)
    }
    else {
      dwEtlLog.warn(s""" -- There are no data from any of the sources for dimension $dimName. The dimension will not change """)
      None
    }

    dfDimProcessedAsOption
  }

  /**
   * On re-runs when the last effective date is used for dimensions with Type 2 columns need to
   * remove all rows from the given effective date onward and recalculate "EndDate" and "MostRecentIndicator"
   *
   * @param dfExistingDim
   * @param effectiveDate
   * @return
   */
  private def removeCurrentAndFutureEffectiveDates(dfExistingDim: DataFrame, effectiveDate: Date): DataFrame = {
    val effDateYYYY_MM_DD = getDateFormatted(effectiveDate, "yyyy-MM-dd")

    val dfDimWithPastEffeciveDates = dfExistingDim
      .filter(s"""StartDate < "$effDateYYYY_MM_DD" """)

    val viewDimWithPastEffectiveDates = dimName + "WithPastEffectiveDates"
    dfDimWithPastEffeciveDates.createOrReplaceTempView(viewDimWithPastEffectiveDates)

    val sqlLatestVersionForCompoundKey
    =
      s"""|
          | SELECT CompoundKey, StartDate
          | FROM (
          |   SELECT ROW_NUMBER() OVER ( PARTITION BY CompoundKey ORDER BY StartDate DESC ) AS ReverseVersionNumber,
          |     CompoundKey,
          |     StartDate
          |   FROM $viewDimWithPastEffectiveDates
          | ) AS DimForCompoundKeySorted
          | WHERE ReverseVersionNumber = 1
          |   """.stripMargin
    val dfLatestVersionForCompoundKey = sparkModelObject.sql(sqlLatestVersionForCompoundKey)

    val dfForChangedCompoundKeys = dfDimWithPastEffeciveDates
      .where("MostRecentIndicator = FALSE") // only touch not latest, so UpdatedOn is not set on the rows that did not change
      .join(dfLatestVersionForCompoundKey, Seq("CompoundKey", "StartDate"), "leftsemi")
      .withColumns(ListMap(
        "MostRecentIndicator" -> {
          val value = lit(true);
          when(value.isNotNull, value).otherwise(lit(null))
        },
        "EndDate" -> {
          val value = to_date(lit(farFutureDateYYYY_MM_DD));
          when(value.isNotNull, value).otherwise(lit(null))
        },
        "UpdatedOn" -> {
          val value = current_timestamp();
          when(value.isNotNull, value).otherwise(lit(null))
        },
        "UpdatedBy" -> {
          val value = lit(System.getProperty("user.name"));
          when(value.isNotNull, value).otherwise(lit(null))
        }
      ))

    val dfUnchagedDimWithPastEffeciveDates = dfDimWithPastEffeciveDates
      .join(dfForChangedCompoundKeys, Seq("CompoundKey", "StartDate"), "leftanti")

    dfForChangedCompoundKeys
      .union(dfUnchagedDimWithPastEffeciveDates)
      .select(dimColsInclEtlOnes.head, dimColsInclEtlOnes.tail: _*) // select columns in the correct order
  }

  /**
   *
   * @param dfExistingDim
   * @param dfNewDim
   * @return ( dfDimWithExistingSurrogKey, dfDimNewWithSurrogKeyNull )
   */
  private def createNewDimVersion(
      dfExistingDim: DataFrame,
      dfNewDim: DataFrame): (DataFrame, DataFrame) = {

    dfExistingDim.createOrReplaceTempView(viewDimExisting)
    dfNewDim.createOrReplaceTempView(viewDimFromStgSources)


    // Only need to build the type one changes only if the dimension has type one columns
    // or when it has neither, i.e., it has natural keys only.
    // In case of natural keys only, it is similar to type one columns because it gets all
    // the distinct instances of natural keys
    val (dfTypeOneExistingCompoundKeyWithSurrogKeySet, dfTypeOneNewCompoundKeyWithSurrogKeyNull) =
      if (hasTypeZeroOneThreeColsOrNaturalKeysOnly) {
        createNewDimVersionForTypeOneChangesOnly(dfExistingDim)
      }
      else {
        (dfEmptyStgSrcDim, dfEmptyStgSrcDim) // this will not have an action on it so there is no impact
      }

    val (dfDimWithExistingSurrogKey, dfDimNewWithSurrogKeyNull) = if (hasTypeTwoCols) {
      val (dfDimTypeTwoExistingCompoundKeySurrogKeySet, dfDimTypeTwoExistingCompoundKeySurrogKeyNull, dfDimTypeTwoNewCompoundKeySurrogKeyNull) = createNewDimVersionForTypeTwoChangesOnly()

      if (isDebugDwLib) {
        dfTypeOneExistingCompoundKeyWithSurrogKeySet.printSchema()
        dfTypeOneNewCompoundKeyWithSurrogKeyNull.printSchema()
        dfDimTypeTwoExistingCompoundKeySurrogKeySet.printSchema()
        dfDimTypeTwoExistingCompoundKeySurrogKeyNull.printSchema()
        dfDimTypeTwoNewCompoundKeySurrogKeyNull.printSchema()

        dfTypeOneExistingCompoundKeyWithSurrogKeySet.show(5)
        dfTypeOneNewCompoundKeyWithSurrogKeyNull.show(5)
        dfDimTypeTwoExistingCompoundKeySurrogKeySet.show(5)
        dfDimTypeTwoExistingCompoundKeySurrogKeyNull.show(5)
        dfDimTypeTwoNewCompoundKeySurrogKeyNull.show(5)

      }

      if (hasTypeOneCols) {
        incorporateTypeOneIntoTypeTwo(
          dfTypeOneExistingCompoundKeyWithSurrogKeySet,
          dfTypeOneNewCompoundKeyWithSurrogKeyNull,
          dfDimTypeTwoExistingCompoundKeySurrogKeySet,
          dfDimTypeTwoExistingCompoundKeySurrogKeyNull,
          dfDimTypeTwoNewCompoundKeySurrogKeyNull,
          dimColsInclEtlOnes)
      }
      else {
        (dfDimTypeTwoExistingCompoundKeySurrogKeySet, dfDimTypeTwoExistingCompoundKeySurrogKeyNull.union(dfDimTypeTwoNewCompoundKeySurrogKeyNull))
      }
    }
    else {
      (dfTypeOneExistingCompoundKeyWithSurrogKeySet, dfTypeOneNewCompoundKeyWithSurrogKeyNull)
    }

    (dfDimWithExistingSurrogKey, dfDimNewWithSurrogKeyNull)
  }

  /**
   *
   * @return - a tuple of two dataframes -
   *           1. compound key existing - surrogate key is set and does not change
   *              2. new compound key - surrogate key is not set
   */

  private def createNewDimVersionForTypeOneChangesOnly(dfExistingDim: DataFrame): (DataFrame, DataFrame) = {
    dwEtlLog.info("-- Creating new dimension with type one changes")

    // Step 1. Create dataframe with types one, zero and three changes from new staging data.
    //        - For type one we take the latest version of the record based on CompoundKey and use that to update type 1 columns,
    //          we ignore all previous effective dates if any.
    //          One side effect of this step is the removal of duplicate keys, so
    //          even if the SQL in dimension loadDim method selects duplicates for the same effective date they will be removed here
    //          and only one row (albeit at random) will be left.
    //         - For types zero and three we create separate LEFT JOIN statements to handle each field separately

    // Exclude type zero and three columns as they will be handled individually
    // This list will be non-empty as dimension has to have at least one natural key (even if there are no any type zero, one or three columns)
    // - enforced by schema validation
    val dimColsExclTypeZeroAndThree: Array[String] = dimColsInclEtlOnes
      .diff(typeZeroCols)
      .diff(typeThreeCols)
    require(!dimColsExclTypeZeroAndThree.isEmpty)

    // build joins and selects for each type zero and three columns
    // each column has to be handled individually because first or last non-zero value can be on different
    // effective days for different columns or not be on any effective day

    val typeZeroAndThreeSelectAndJoin = for ((typeZeroOrThreeCol, scdType) <- (typeZeroCols zip List.fill(typeZeroCols.length)(0))
      // .concat(typeThreeCols zip List.fill(typeThreeCols.length)(3))    // Scala 2.13
      .union(typeThreeCols zip List.fill(typeThreeCols.length)(3))        //  refactored for Scala 2.12 - replaced with union and added distinct
      .distinct
                                             )
      yield (s"srcFor$typeZeroOrThreeCol.$typeZeroOrThreeCol AS $typeZeroOrThreeCol",

        // Sort by a type zero or three colun in a way to place non-null values first and then filter by effective day.
        // This way a non-null value will be selected if available for some earliest or latest effective date (depending
        // on whether it is type zero or type three column)
        s"""ROW_NUMBER() OVER ( PARTITION BY CompoundKey ORDER BY IF( $typeZeroOrThreeCol IS NULL, 1, 0) ASC, LoadEffectiveDate ${if (scdType == 0) "ASC" else "DESC"} ) AS VerNum$typeZeroOrThreeCol, """,

        s"""LEFT OUTER JOIN ( srcDimPlaceHolder )  AS srcFor$typeZeroOrThreeCol    ON      srcDim.CompoundKey = srcFor$typeZeroOrThreeCol.CompoundKey
           |                                                            AND srcFor$typeZeroOrThreeCol.VerNum$typeZeroOrThreeCol = 1
           |""".stripMargin,
        // this was tested before removing CTEs. It will not be used - keep it as a potential option - will need re-test if incorporated
        s"""LEFT OUTER JOIN ( SELECT $typeZeroOrThreeCol,
           |                       CompoundKey,
           |                       ROW_NUMBER() OVER ( PARTITION BY CompoundKey ORDER BY LoadEffectiveDate ${if (scdType == 0) "ASC" else "DESC"} ) AS VersionNumber
           |                 FROM $viewDimFromStgSources
           |                 WHERE $typeZeroOrThreeCol IS NOT NULL ) AS srcFor$typeZeroOrThreeCol    ON      srcDim.CompoundKey = srcFor$typeZeroOrThreeCol.CompoundKey
           |                                                                               AND srcFor$typeZeroOrThreeCol.VersionNumber = 1
           |""".stripMargin

      )

    val srcDim =
      s"""
         |SELECT ROW_NUMBER() OVER ( PARTITION BY CompoundKey ORDER BY LoadEffectiveDate DESC ) AS VersionNumber,
         |       *
         |  FROM $viewDimFromStgSources
         |""".stripMargin


    val sqlWithTypeOneChanges =
      s"""|
          |SELECT ${dimColsExclTypeZeroAndThree.mkString("\n  srcDim.", ",\n  srcDim.", if (hasTypeZeroOrThreeCols) "," else "")}
          |       ${typeZeroAndThreeSelectAndJoin.map(_._1).mkString("\n ,")}
          |FROM (
          |  $srcDim
          |  ) AS srcDim
          |      ${typeZeroAndThreeSelectAndJoin.map(_._4).mkString("\n")}
				  |WHERE srcDim.VersionNumber = 1
          |""".stripMargin

    dwEtlLog.info("-- Executing step 1 for Type one changes " + sqlWithTypeOneChanges)

    val dfWithTypeOneChanges = sparkModelObject.sql(sqlWithTypeOneChanges)
      // .pipe(q => if (hasTypeZeroOrThreeCols) q.select(dimColsInclEtlOnes.head, dimColsInclEtlOnes.tail: _*) else q) // The optional call to select columns in the right order
      .cache()
    if (isDebugDwLib) {
      dfWithTypeOneChanges.show(50);
      dfWithTypeOneChanges.printSchema()
    }

    val viewDimWithTypeOneChanges = dimName + "WithTypeOneChanges"
    dfWithTypeOneChanges.createOrReplaceTempView(viewDimWithTypeOneChanges)

    // Step 2. Create data frames that when unioned will create the dimension
    //          with latest versions of each compound key with type one changes

    // Step 2-1. Create dataframe with Unchanged Most Recent Versions
    val sqlForCompoundKeyInExistingDimOnly =
      s"""
         |SELECT	${dimColsInclEtlOnes.mkString("\n  dim.", ",\n  dim.", "")}
         |FROM $viewDimExisting AS dim
         |WHERE dim.MostRecentIndicator = TRUE    -- need this in case the dimension has type 2 columns
         |  AND NOT EXISTS ( SELECT 1 FROM $viewDimWithTypeOneChanges AS new
         |						        WHERE dim.CompoundKey = new.CompoundKey )
         |""".stripMargin

    dwEtlLog.info("-- Executing step 2-1 for Type one changes " + sqlForCompoundKeyInExistingDimOnly)

    val dfForCompoundKeyInExistingDimOnly = sparkModelObject.sql(sqlForCompoundKeyInExistingDimOnly)
    if (isDebugDwLib) {
      dfForCompoundKeyInExistingDimOnly.show(150);
      dfForCompoundKeyInExistingDimOnly.printSchema()
    }

    // There used to be two steps to genertae new dimension - one when the data did not change,
    // i.e., when dim.TypeOneColumnsHash	= new.TypeOneColumnsHash and the second when type one data changed,
    // i.e., dim.TypeOneColumnsHash	!= new.TypeOneColumnsHash . Based on understanding that it is better
    // - for performance and simplicity - to have fewer steps (and as a result fewer dataframes)
    // combine these two steps into one. The other reason to combine is that to process type zero and type three
    // I would need to inroduce the same logic to both steps.

    // here ExistingCompoundKey meaning existing in both existing dimension and in new data
    val typeZeroAndThreeSelectForTheExistingCompoundKey = for ((typeZeroOrThreeCol, scdType)
                                                                 <- (typeZeroCols zip List.fill(typeZeroCols.length)(0))
      // .concat(typeThreeCols zip List.fill(typeThreeCols.length)(3))  // Scala 2.13
      .union(typeThreeCols zip List.fill(typeThreeCols.length)(3))      //  refactored for Scala 2.12 - replaced with union and added distinct
      .distinct
                                                               )
    yield (
      if (scdType == 0)
        s"IFNULL( dim.$typeZeroOrThreeCol, new.$typeZeroOrThreeCol ) AS $typeZeroOrThreeCol" // populate from new data only if NULL in dimension
      else if (scdType == 3)
        s"IFNULL( new.$typeZeroOrThreeCol, dim.$typeZeroOrThreeCol ) AS $typeZeroOrThreeCol" // populate type three from new data if NOT NULL regardless of what in dimension
      else
        throw new RuntimeException("Unexpected scd type while processing dimension")
      )

    // Step 2-2. Create data frame with the same or changed version for existing compound key
    //          Meaning existing in both existing dimension and in new data
    val sqlForTheExistingCompoundKey =
      s"""
         |SELECT	${dimColsExclTypeZeroAndThree.mkString("\n  new.", ",\n  new.", if (hasTypeZeroOrThreeCols) "," else "")}
         |        ${typeZeroAndThreeSelectForTheExistingCompoundKey.mkString("\n ,")}
         |FROM $viewDimExisting AS dim
         |	INNER JOIN $viewDimWithTypeOneChanges AS new		ON		dim.CompoundKey			= new.CompoundKey
         |                        										-- AND dim.TypeOneColumnsHash	= new.TypeOneColumnsHash -- combined changed and unchanged data for type one columns
         |WHERE dim.MostRecentIndicator = TRUE
         |""".stripMargin
        .replace(s"new.$dimKey", s"dim.$dimKey") // Take one column - the key - from dimension (every other column comes from new data, except for types zero and three
    dwEtlLog.info("-- Executing step 2-2 for Type one changes " + sqlForTheExistingCompoundKey)

    // Scala 2.13 - is using scala.util.chaining._
/*
    val dfForTheExistingCompoundKey = spark.sql(sqlForTheExistingCompoundKey)
      .pipe(q => if (hasTypeZeroOrThreeCols) q.select(dimColsInclEtlOnes.head, dimColsInclEtlOnes.tail: _*) else q) // The optional call to select columns in the right order
*/
    // Refactored for Scala 2.12
    val dfForTheExistingCompoundKey = if (hasTypeZeroOrThreeCols)
      sparkModelObject.sql(sqlForTheExistingCompoundKey)
        .select(dimColsInclEtlOnes.head, dimColsInclEtlOnes.tail: _*)
    else
      sparkModelObject.sql(sqlForTheExistingCompoundKey)


    if (isDebugDwLib) {
      dfForTheExistingCompoundKey.show(150);
      dfForTheExistingCompoundKey.printSchema()
    }

    // Step 2-3. Create data frame for changed version for the existing compound key
    /*
        val sqlChangedVersionForTheExistingCompoundKey =
          s"""
             |SELECT	${dimColsInclEtlOnes.mkString("\n  new.", ",\n  new.", "")}
             |FROM $viewDimExisting AS dim
             |	INNER JOIN $viewDimWithTypeOneChanges AS new		ON		dim.CompoundKey			= new.CompoundKey
             |                        										AND dim.TypeOneColumnsHash != new.TypeOneColumnsHash
             |WHERE dim.MostRecentIndicator = TRUE
             |""".stripMargin
            .replace(s"new.$dimKey", s"dim.$dimKey") // Take one column - the key - from dimension (every other column comes from new data.
        dwEtlLog.info("-- Executing step 2-3 for Type one changes " + sqlChangedVersionForTheExistingCompoundKey)

        val dfChangedVersionForTheExistingCompoundKey = spark.sql(sqlChangedVersionForTheExistingCompoundKey)
        if (isDebugDwLib) {
          dfChangedVersionForTheExistingCompoundKey.show(150);
          dfChangedVersionForTheExistingCompoundKey.printSchema()
        }
    */

    // Step 2-4. Create data frame with new compound key
    // For Boolean compare both = and == work. Tested in the spark-shell
    val dfNewCompoundKey = dfWithTypeOneChanges.join(dfExistingDim.where("MostRecentIndicator = TRUE"),
        Seq("CompoundKey"), "leftanti")
      .select(dimColsInclEtlOnes.head, dimColsInclEtlOnes.tail: _*) // "leftanti" moves the CompoundKey to the front - so select columns in correct order
    dwEtlLog.info("-- Executing step 2-4 for Type one changes")

    if (isDebugDwLib) {
      dfNewCompoundKey.show(5);
      dfNewCompoundKey.printSchema()
    }

    val dfExistingCompoundKey = dfForCompoundKeyInExistingDimOnly
      .union(dfForTheExistingCompoundKey)

    if (isDebugDwLib) {
      dfExistingCompoundKey.show(150);
      dfExistingCompoundKey.printSchema()
    }

    (dfExistingCompoundKey, dfNewCompoundKey)
  }

  /**
   *
   * @return - a tuple of three dataframes -
   *         1. existing Compound key, surrogate key is set
   *            2. existing Compound key, surrogate key is not set
   *            3. new Compound key
   */

  private def createNewDimVersionForTypeTwoChangesOnly(): (DataFrame, DataFrame, DataFrame) = {
    dwEtlLog.info("-- Creating new dimension with type 2 changes only")

    // Step 1. Create dataframe with type two changes from new staging data
    //         srcDimToRemoveDuplicates insures that duplicate roes for the same key
    //         and effective date are removed (albeit at random).

    // The original version had many CTEs which Spark does not like
    // replace with in-line SQL
    val srcDimToRemoveDuplicates =
      s"""
         |SELECT ROW_NUMBER() OVER ( PARTITION BY CompoundKey, LoadEffectiveDate
         |                             ORDER BY LoadEffectiveDate DESC ) AS KeysEffDatesOrdered,   -- have the same order as for type 1 , so if this dimension is type 4, the same rows will be selected as for type 1 dimension
         |       *
         |FROM $viewDimFromStgSources
         |""".stripMargin
    val srcDim =
      s"""
         |SELECT ROW_NUMBER() OVER ( PARTITION BY CompoundKey
         |                             ORDER BY LoadEffectiveDate ASC ) AS VersionNumber,
         |       *
         |FROM ( $srcDimToRemoveDuplicates )
         |WHERE KeysEffDatesOrdered = 1
         |""".stripMargin
    val prevVersions =
      s"""
         |SELECT currDate.TypeTwoColumnsHash							        AS CurrDateType2ColHash,
         |       IFNULL( prevDate.TypeTwoColumnsHash, "-" )			  AS PrevDateType2ColHash,
         |       IF( currDate.VersionNumber = 1, TRUE, FALSE )   AS FirstNewVersion,
         |       currDate.*
         |FROM ( $srcDim ) AS currDate
         |	        LEFT OUTER JOIN ( $srcDim ) AS prevDate ON		currDate.CompoundKey	= prevDate.CompoundKey
         |					        				      		                AND currDate.VersionNumber	= prevDate.VersionNumber + 1
         |""".stripMargin
    val type2Sorted =
      s"""
         |SELECT ROW_NUMBER() OVER ( PARTITION BY CompoundKey
         |                             ORDER BY VersionNumber ASC ) AS VersionOfType2,
         |		*
         |FROM ( $prevVersions ) AS currDate
         |WHERE CurrDateType2ColHash != PrevDateType2ColHash
         |""".stripMargin

    val sqlWithTypeTwoChanges =
      s"""
         |SELECT	currDate.LoadEffectiveDate											AS CurrEffectiveDate,
         |		IFNULL( DATE_SUB( nextDate.LoadEffectiveDate, 1 ), '$farFutureDateYYYY_MM_DD'	)	AS NextEffectiveDate,
         |		IF( nextDate.LoadEffectiveDate IS NULL, TRUE, FALSE )				AS CalculatedMostRecentIndicator,
         |		currDate.*
         |FROM ( $type2Sorted ) AS currDate
         |	LEFT OUTER JOIN ( $type2Sorted ) AS   nextDate ON	currDate.CompoundKey	= nextDate.CompoundKey
         |											              AND currDate.VersionOfType2	= nextDate.VersionOfType2 - 1
         |""".stripMargin

    dwEtlLog.info("-- Executing step 1 for Type 2 changes " + sqlWithTypeTwoChanges)

    val dfWithTypeTwoChanges = sparkModelObject.sql(sqlWithTypeTwoChanges)
      .drop("KeysEffDatesOrdered")
      .cache()
    if (isDebugDwLib) {
      // dfPlayByPlay.show( 5);
      dfWithTypeTwoChanges.show(5);
      dfWithTypeTwoChanges.printSchema()
    }

    val viewDimWithTypeTwoChanges = dimName + "WithTypeTwoChanges"
    dfWithTypeTwoChanges.createOrReplaceTempView(viewDimWithTypeTwoChanges)

    // Step 2. Create dataframes that when unioned will create the latest dimension
    //          with Type changes incorporated

    // Step 2-1. Create dataframe with Unchanged Non-Most Recent Versions
    val sqlNonMostRecentVersionsForTheExistingCompoundKey =
      s"""
         |SELECT	${dimColsInclEtlOnes.mkString("\n  ", ",\n  ", "")}
         |FROM $viewDimExisting
         |WHERE MostRecentIndicator = FALSE
         |""".stripMargin

    dwEtlLog.info("-- Executing step 2-1 for Type 2 changes " + sqlNonMostRecentVersionsForTheExistingCompoundKey)

    val dfNonMostRecentVersionsForTheExistingCompoundKey = sparkModelObject.sql(sqlNonMostRecentVersionsForTheExistingCompoundKey)
    if (isDebugDwLib) {
      dfNonMostRecentVersionsForTheExistingCompoundKey.show(5);
      dfNonMostRecentVersionsForTheExistingCompoundKey.printSchema()
    }

    // Step 2-2. Create dataframe with Unchanged Most Recent Versions
    val sqlUnchangedMostRecentVersionsForTheExistingCompoundKey =
      s"""
         |SELECT	${dimColsInclEtlOnes.mkString("\n  dim.", ",\n  dim.", "")}
         |FROM $viewDimExisting AS dim
         |WHERE dim.MostRecentIndicator = TRUE
         |  AND NOT EXISTS ( SELECT 1 FROM $viewDimWithTypeTwoChanges AS new
         |						        WHERE dim.CompoundKey = new.CompoundKey )
         |""".stripMargin

    dwEtlLog.info("-- Executing step 2-2 for Type 2 changes " + sqlUnchangedMostRecentVersionsForTheExistingCompoundKey)

    val dfUnchangedMostRecentVersionsForTheExistingCompoundKey = sparkModelObject.sql(sqlUnchangedMostRecentVersionsForTheExistingCompoundKey)
    if (isDebugDwLib) {
      dfUnchangedMostRecentVersionsForTheExistingCompoundKey.show(5);
      dfUnchangedMostRecentVersionsForTheExistingCompoundKey.printSchema()
    }

    // Step 2-3. Create data frame with later versions from new sources
    val sqlLaterVersionsOfExistingCompoundKey =
      s"""
         |SELECT		${dimCols.mkString("\n  newSecondAndLaterVersions.", ",\n  newSecondAndLaterVersions.", "")},
         |  newSecondAndLaterVersions.CompoundKey,
         |  newSecondAndLaterVersions.TypeOneColumnsHash,
         |  newSecondAndLaterVersions.CurrDateType2ColHash	AS TypeTwoColumnsHash,
         |  newSecondAndLaterVersions.SourceSystem,
         |  newSecondAndLaterVersions.LoadEffectiveDate,
         |  newSecondAndLaterVersions.CreatedOn,
         |  newSecondAndLaterVersions.CreatedBy,
         |  newSecondAndLaterVersions.CurrEffectiveDate		AS StartDate,
         |	 newSecondAndLaterVersions.NextEffectiveDate		AS EndDate,
         |	 newSecondAndLaterVersions.CalculatedMostRecentIndicator AS MostRecentIndicator,
         |  newSecondAndLaterVersions.UpdatedOn,
         |  newSecondAndLaterVersions.UpdatedBy
         |FROM $viewDimWithTypeTwoChanges AS newSecondAndLaterVersions
         |    INNER JOIN $viewDimExisting AS dim ON newSecondAndLaterVersions.CompoundKey			= dim.CompoundKey
         |WHERE newSecondAndLaterVersions.FirstNewVersion = FALSE AND dim.MostRecentIndicator = TRUE   -- compound key from existing dimension
         |""".stripMargin
    dwEtlLog.info("-- Executing step 2-3 for Type 2 changes " + sqlLaterVersionsOfExistingCompoundKey)

    val dfLaterVersionsOfExistingCompoundKey = sparkModelObject.sql(sqlLaterVersionsOfExistingCompoundKey)
    if (isDebugDwLib) {
      dfLaterVersionsOfExistingCompoundKey.show(5);
      dfLaterVersionsOfExistingCompoundKey.printSchema()
    }

    // Step 2-4. Create dataframe with the same version for the existing compound key
    val sqlTheSameVersionForTheExistingCompoundKey =
      s"""
         |SELECT		${dimCols.mkString("\n  dim.", ",\n  dim.", "")},
         |  dim.CompoundKey,
         |  dim.TypeOneColumnsHash,
         |  dim.TypeTwoColumnsHash,
         |  dim.SourceSystem,
         |  dim.LoadEffectiveDate,
         |  dim.CreatedOn,
         |  dim.CreatedBy,
         |  dim.StartDate,
         |	 new.NextEffectiveDate		         AS EndDate,
         |	 new.CalculatedMostRecentIndicator AS MostRecentIndicator,
         |   IF( dim.EndDate = new.NextEffectiveDate AND dim.MostRecentIndicator = new.CalculatedMostRecentIndicator, dim.UpdatedOn, new.UpdatedOn ) AS UpdatedOn,
         |   IF( dim.EndDate = new.NextEffectiveDate AND dim.MostRecentIndicator = new.CalculatedMostRecentIndicator, dim.UpdatedBy, new.UpdatedBy ) AS UpdatedBy
         |FROM $viewDimExisting AS dim
         |	INNER JOIN $viewDimWithTypeTwoChanges AS new		ON		dim.CompoundKey			= new.CompoundKey
         |										AND dim.TypeTwoColumnsHash	= new.CurrDateType2ColHash
         |WHERE dim.MostRecentIndicator = TRUE AND new.FirstNewVersion = TRUE
         |""".stripMargin
    dwEtlLog.info("-- Executing step 2-4 for Type 2 changes " + sqlTheSameVersionForTheExistingCompoundKey)

    val dfTheSameVersionForTheExistingCompoundKey = sparkModelObject.sql(sqlTheSameVersionForTheExistingCompoundKey)
    if (isDebugDwLib) {
      dfTheSameVersionForTheExistingCompoundKey.show(5);
      dfTheSameVersionForTheExistingCompoundKey.printSchema()
    }

    // Step 2-5. Create data frame with expired version for the existing compound key
    val sqlExpiredVersionForTheExistingCompoundKey =
      s"""
         |SELECT		${dimCols.mkString("\n  dim.", ",\n  dim.", "")},
         |  dim.CompoundKey,
         |  dim.TypeOneColumnsHash,
         |  dim.TypeTwoColumnsHash,
         |  dim.SourceSystem,
         |  dim.LoadEffectiveDate,
         |  dim.CreatedOn,
         |  dim.CreatedBy,
         |  dim.StartDate,
         |	 DATE_SUB( new.CurrEffectiveDate, 1 )		AS EndDate,
         |	 FALSE                                  AS MostRecentIndicator,
         |   new.UpdatedOn,
         |   new.UpdatedBy
         |FROM $viewDimExisting AS dim
         |	INNER JOIN $viewDimWithTypeTwoChanges AS new		ON		dim.CompoundKey			= new.CompoundKey
         |										AND dim.TypeTwoColumnsHash	!= new.CurrDateType2ColHash         -- this condition is different from the same version for the existing compound key
         |WHERE dim.MostRecentIndicator = TRUE AND new.FirstNewVersion = TRUE
         |""".stripMargin
    dwEtlLog.info("-- Executing step 2-5 for Type 2 changes " + sqlExpiredVersionForTheExistingCompoundKey)

    val dfExpiredVersionForTheExistingCompoundKey = sparkModelObject.sql(sqlExpiredVersionForTheExistingCompoundKey)
    if (isDebugDwLib) {
      dfExpiredVersionForTheExistingCompoundKey.show(5);
      dfExpiredVersionForTheExistingCompoundKey.printSchema()
    }


    // Step 2-6. Create data frame with new version for the existing compound key
    //            The JOIN and the where clause is the same as for expired version for
    //            the existing compound key. We only take the data from new data
    //            as opposed to existing dimension
    val sqlNewVersionForTheExistingCompoundKey =
      s"""
         |SELECT		${dimCols.mkString("\n  new.", ",\n  new.", "")},
         |  new.CompoundKey,
         |  new.TypeOneColumnsHash,
         |  new.CurrDateType2ColHash          AS TypeTwoColumnsHash,
         |  new.SourceSystem,
         |  new.LoadEffectiveDate,
         |  new.CreatedOn,
         |  new.CreatedBy,
         |  new.CurrEffectiveDate		          AS StartDate,
         |	new.NextEffectiveDate		          AS EndDate,
         |	new.CalculatedMostRecentIndicator AS MostRecentIndicator,
         |  new.UpdatedOn,
         |  new.UpdatedBy
         |FROM $viewDimExisting AS dim
         |	INNER JOIN $viewDimWithTypeTwoChanges AS new		ON		dim.CompoundKey			= new.CompoundKey
         |										AND dim.TypeTwoColumnsHash	!= new.CurrDateType2ColHash
         |WHERE dim.MostRecentIndicator = TRUE AND new.FirstNewVersion = TRUE
         |""".stripMargin

    dwEtlLog.info("-- Executing step 2-6 for Type 2 changes " + sqlNewVersionForTheExistingCompoundKey)

    val dfNewVersionForTheExistingCompoundKey = sparkModelObject.sql(sqlNewVersionForTheExistingCompoundKey)
    if (isDebugDwLib) {
      dfNewVersionForTheExistingCompoundKey.show(5);
      dfNewVersionForTheExistingCompoundKey.printSchema()
    }

    // Step 2-7. Create data frame with new compound key
    val sqlNewCompoundKey =
      s"""
         |
         |SELECT		${dimCols.mkString("\n  new.", ",\n  new.", "")},
         |  new.CompoundKey,
         |  new.TypeOneColumnsHash,
         |  new.CurrDateType2ColHash          AS TypeTwoColumnsHash,
         |  new.SourceSystem,
         |  new.LoadEffectiveDate,
         |  new.CreatedOn,
         |  new.CreatedBy,
         |  IF( new.FirstNewVersion = TRUE, CAST( '$farPastDateYYYY_MM_DD' AS DATE ), new.CurrEffectiveDate )        AS StartDate,
         |	new.NextEffectiveDate		                        AS EndDate,
         |	new.CalculatedMostRecentIndicator               AS MostRecentIndicator,
         |  new.UpdatedOn,
         |  new.UpdatedBy
         |FROM $viewDimWithTypeTwoChanges AS new
         |WHERE NOT EXISTS (	SELECT 1 FROM $viewDimExisting AS dim
         |						        WHERE dim.CompoundKey = new.CompoundKey AND dim.MostRecentIndicator = TRUE )
         |""".stripMargin
    dwEtlLog.info("-- Executing step 2-7 for Type 2 changes " + sqlNewCompoundKey)

    val dfNewCompoundKey = sparkModelObject.sql(sqlNewCompoundKey)
    if (isDebugDwLib) {
      dfNewCompoundKey.show(5);
      dfNewCompoundKey.printSchema()
    }

    //    spark.catalog.dropTempView( viewDimWithTypeTwoChanges )

    (dfNonMostRecentVersionsForTheExistingCompoundKey
      .union(dfUnchangedMostRecentVersionsForTheExistingCompoundKey)
      .union(dfTheSameVersionForTheExistingCompoundKey)
      .union(dfExpiredVersionForTheExistingCompoundKey),
      dfLaterVersionsOfExistingCompoundKey
        .union(dfNewVersionForTheExistingCompoundKey),
      dfNewCompoundKey)
  }

  private def incorporateTypeOneIntoTypeTwo(
      dfTypeOneExistingCompoundKeyWithSurrogKeySet: DataFrame,
      dfTypeOneNewCompoundKeyWithSurrogKeyNull: DataFrame,
      dfDimTypeTwoExistingCompundKeySurrogKeySet: DataFrame,
      dfDimTypeTwoExistingCompundKeySurrogKeyNull: DataFrame,
      dfDimTypeTwoNewCompoundKeySurrogKeyNull: DataFrame,
      dimColumnsInclEtlCols: Array[String]): (DataFrame, DataFrame) = {

    val viewTypeOneExistingCompoundKeyWithSurrogKeySet = dimName + "TypeOneExistingCompoundKeyWithSurrogKeySet"
    val viewTypeOneNewCompoundKeyWithSurrogKeyNull = dimName + "TypeOneNewCompoundKeyWithSurrogKeyNull"
    val viewDimTypeTwoExistingCompundKeySurrogKeySet = dimName + "DimTypeTwoExistingCompundKeySurrogKeySet"
    val viewDimTypeTwoExistingCompundKeySurrogKeyNull = dimName + "DimTypeTwoExistingCompundKeySurrogKeyNull"
    val viewDimTypeTwoNewCompoundKeySurrogKeyNull = dimName + "DimTypeTwoNewCompoundKeySurrogKeyNull"

    dfTypeOneExistingCompoundKeyWithSurrogKeySet.createOrReplaceTempView(viewTypeOneExistingCompoundKeyWithSurrogKeySet)
    dfTypeOneNewCompoundKeyWithSurrogKeyNull.createOrReplaceTempView(viewTypeOneNewCompoundKeyWithSurrogKeyNull)
    dfDimTypeTwoExistingCompundKeySurrogKeySet.createOrReplaceTempView(viewDimTypeTwoExistingCompundKeySurrogKeySet)
    dfDimTypeTwoExistingCompundKeySurrogKeyNull.createOrReplaceTempView(viewDimTypeTwoExistingCompundKeySurrogKeyNull)
    dfDimTypeTwoNewCompoundKeySurrogKeyNull.createOrReplaceTempView(viewDimTypeTwoNewCompoundKeySurrogKeyNull)

    if (isDebugDwLib) {
      dfTypeOneNewCompoundKeyWithSurrogKeyNull.show(100)
      dfDimTypeTwoNewCompoundKeySurrogKeyNull.show(100)
      dfTypeOneNewCompoundKeyWithSurrogKeyNull.printSchema()
      dfDimTypeTwoNewCompoundKeySurrogKeyNull.printSchema()
    }

    // create concatenated list of fields for select
    // Since column names are the same in type one and type two sets I can use
    // typeOne columns as it is just the column names - not column values.
    // Here I need only type one column names, but the value can be populated either from type one
    // or type two tables. The idea here to set data from type two row if there is no corresponding row in type one data set.
    // That will be indicated by typeOne.CompoundKey being NULL since CompoundKey can never be NULL on a dimension row
    val ifNullTypeOneElseTypeTwo = for (typesZeroOneOrThreeCol <- typeOneCols
      // .concat(typeZeroCols)       // Scala 2.13
      .union(typeZeroCols)
      .distinct                            //  refactored for Scala 2.12 - replaced with union and added distinct
      // .concat(typeThreeCols)
      .union(typeThreeCols)
      .distinct
                                        )
      yield s"IF( typeOne.CompoundKey IS NOT NULL, typeOne.$typesZeroOneOrThreeCol, typeTwo.$typesZeroOneOrThreeCol ) AS $typesZeroOneOrThreeCol"

    val sqlTypeOneColumnChangedForExistingWithSurrogKeySet =
      s"""
         |
         |SELECT
				 |  typeTwo.$dimKey,
				 |	${naturalKeys.mkString("\n  typeTwo.", ",\n  typeTwo.", "")},
         |	${typeTwoCols.mkString("\n  typeTwo.", ",\n  typeTwo.", "")},
         |  ${ifNullTypeOneElseTypeTwo.mkString("\n", ",\n", "")},
         |  typeTwo.CompoundKey,
         |  IF( typeOne.CompoundKey IS NOT NULL, typeOne.TypeOneColumnsHash, typeTwo.TypeOneColumnsHash ) AS TypeOneColumnsHash,
         |  typeTwo.TypeTwoColumnsHash,
         |  typeTwo.SourceSystem,
         |  typeTwo.LoadEffectiveDate,
         |  typeTwo.CreatedOn,
         |  typeTwo.CreatedBy,
         |  typeTwo.StartDate,
         |	typeTwo.EndDate,
         |	typeTwo.MostRecentIndicator,
         |  IF( typeOne.CompoundKey IS NOT NULL, typeOne.UpdatedOn, typeTwo.UpdatedOn ) AS UpdatedOn,
         |  IF( typeOne.CompoundKey IS NOT NULL, typeOne.UpdatedBy, typeTwo.UpdatedBy ) AS UpdatedBy
         |FROM $viewDimTypeTwoExistingCompundKeySurrogKeySet AS typeTwo
				 |  LEFT OUTER JOIN $viewTypeOneExistingCompoundKeyWithSurrogKeySet AS typeOne  ON   typeTwo.CompoundKey = typeOne.CompoundKey
				 |                                                           --        AND typeTwo.TypeOneColumnsHash != typeOne.TypeOneColumnsHash -- removed this condition as it only restricts the values on LEFT JOIN. Need it to allow type zero and three be set correctly since they do not have hash
         |""".stripMargin
    dwEtlLog.info("-- Applying Type 1 changes to Type 2 for existing compound key with surrogate key set " + sqlTypeOneColumnChangedForExistingWithSurrogKeySet)

    val dfTypeOneColumnChangedForExistingWithSurrogKeySet = sparkModelObject.sql(sqlTypeOneColumnChangedForExistingWithSurrogKeySet)
      .select(dimColumnsInclEtlCols.head, dimColumnsInclEtlCols.tail: _*) // select columns in the correct order

    if (isDebugDwLib) {
      dfTypeOneColumnChangedForExistingWithSurrogKeySet.show(5);
      dfTypeOneColumnChangedForExistingWithSurrogKeySet.printSchema()
    }

    val sqlTypeOneColumnChangedForExistingWithSurrogKeyNull =
      s"""
         |
         |SELECT
         |  typeTwo.$dimKey,
         |	${naturalKeys.mkString("\n  typeTwo.", ",\n  typeTwo.", "")},
         |	${typeTwoCols.mkString("\n  typeTwo.", ",\n  typeTwo.", "")},
         |  ${ifNullTypeOneElseTypeTwo.mkString("\n", ",\n", "")},
         |  typeTwo.CompoundKey,
         |  IF( typeOne.CompoundKey IS NOT NULL, typeOne.TypeOneColumnsHash, typeTwo.TypeOneColumnsHash ) AS TypeOneColumnsHash,
         |  typeTwo.TypeTwoColumnsHash,
         |  typeTwo.SourceSystem,
         |  typeTwo.LoadEffectiveDate,
         |  typeTwo.CreatedOn,
         |  typeTwo.CreatedBy,
         |  typeTwo.StartDate,
         |	typeTwo.EndDate,
         |	typeTwo.MostRecentIndicator,
         |  IF( typeOne.CompoundKey IS NOT NULL, typeOne.UpdatedOn, typeTwo.UpdatedOn ) AS UpdatedOn,
         |  IF( typeOne.CompoundKey IS NOT NULL, typeOne.UpdatedBy, typeTwo.UpdatedBy ) AS UpdatedBy
         |FROM $viewDimTypeTwoExistingCompundKeySurrogKeyNull AS typeTwo
         |  LEFT OUTER JOIN $viewTypeOneExistingCompoundKeyWithSurrogKeySet AS typeOne  ON    typeTwo.CompoundKey = typeOne.CompoundKey
         |                                                          --  AND typeTwo.TypeOneColumnsHash != typeOne.TypeOneColumnsHash -- removed this condition as it only restricts the values on LEFT JOIN. Need it to allow type zero and three be set correctly since they do not have hash
         |""".stripMargin
    dwEtlLog.info("-- Applying Type 1 changes to Type 2 for existing compound key with surrogate key NULL " + sqlTypeOneColumnChangedForExistingWithSurrogKeyNull)

    val dfTypeOneColumnChangedForExistingWithSurrogKeyNull = sparkModelObject.sql(sqlTypeOneColumnChangedForExistingWithSurrogKeyNull)
      .select(dimColumnsInclEtlCols.head, dimColumnsInclEtlCols.tail: _*) // select columns in the correct order

    if (isDebugDwLib) {
      dfTypeOneColumnChangedForExistingWithSurrogKeyNull.printSchema()
      dfTypeOneColumnChangedForExistingWithSurrogKeyNull.show(5);
    }


    // For new can use inner join as the rows must exist for a compound key on both type 1 and type 2 sets
    val sqlTypeOneColumnChangedForNewCompoundKeySurrogKeyNull =
      s"""
         |SELECT
         |  typeTwo.$dimKey,
         |	${naturalKeys.mkString("\n  typeTwo.", ",\n  typeTwo.", "")},
         |	${typeTwoCols.mkString("\n  typeTwo.", ",\n  typeTwo.", "")},
         |  ${ifNullTypeOneElseTypeTwo.mkString("\n", ",\n", "")},
         |  typeTwo.CompoundKey,
         |  IF( typeOne.CompoundKey IS NOT NULL, typeOne.TypeOneColumnsHash, typeTwo.TypeOneColumnsHash ) AS TypeOneColumnsHash,
         |  typeTwo.TypeTwoColumnsHash,
         |  typeTwo.SourceSystem,
         |  typeTwo.LoadEffectiveDate,
         |  typeTwo.CreatedOn,
         |  typeTwo.CreatedBy,
         |  typeTwo.StartDate,
         |	typeTwo.EndDate,
         |	typeTwo.MostRecentIndicator,
         |  IF( typeOne.CompoundKey IS NOT NULL, typeOne.UpdatedOn, typeTwo.UpdatedOn ) AS UpdatedOn,
         |  IF( typeOne.CompoundKey IS NOT NULL, typeOne.UpdatedBy, typeTwo.UpdatedBy ) AS UpdatedBy
         |FROM $viewDimTypeTwoNewCompoundKeySurrogKeyNull AS typeTwo
         |  LEFT OUTER JOIN $viewTypeOneNewCompoundKeyWithSurrogKeyNull AS typeOne  ON   typeTwo.CompoundKey = typeOne.CompoundKey
         |                                                           --     AND typeTwo.TypeOneColumnsHash != typeOne.TypeOneColumnsHash  -- removed this condition as it only restricts the values on LEFT JOIN. Need it to allow type zero and three be set correctly since they do not have hash
         |""".stripMargin
    dwEtlLog.info("-- Applying Type 1 changes to Type 2 for new compound key with surrogate key NULL " + sqlTypeOneColumnChangedForNewCompoundKeySurrogKeyNull)

    val dfTypeOneColumnChangedForNewCompoundKeySurrogKeyNull = sparkModelObject.sql(sqlTypeOneColumnChangedForNewCompoundKeySurrogKeyNull)
      .select(dimColumnsInclEtlCols.head, dimColumnsInclEtlCols.tail: _*) // select columns in the correct order

    if (isDebugDwLib) {
      dfTypeOneColumnChangedForNewCompoundKeySurrogKeyNull.printSchema()
      dfTypeOneColumnChangedForNewCompoundKeySurrogKeyNull.show(5);
    }

    val (dfDimWithExistingSurrogKey, dfDimNewWithSurrogKeyNull) =
      (dfTypeOneColumnChangedForExistingWithSurrogKeySet, dfTypeOneColumnChangedForExistingWithSurrogKeyNull.union(dfTypeOneColumnChangedForNewCompoundKeySurrogKeyNull))

    (dfDimWithExistingSurrogKey, dfDimNewWithSurrogKeyNull)
  }

  /**
   *
   * @return - if empty dataset is returned it means that the derived class most likely did not override
   *         def loadDim( stgSrcViewWithMonikerName: String ): DataFrame = empty
   *
   *         this means that the derived dimension did not override loadDim without effective date.
   *         That will trigger the call to the loadDim with effective date
   *         if the dimension did not also override loadDim( effDateYYYY_MM_DD, stgSrcViewWithMonikerName )
   *         it will result in exception later on
   */
  private def doLoadForAllSources(
      stgSrcViewsWithMonikerNames: List[String],
      effDateYYYY_MM_DD: String = null): Option[DataFrame] = {

    // loop though all sources to load the dimension and merge data from each source into a single result
    // Sources earlier in the list have precedence over the ones later in the list
    val dfDimsFromAllSourcesAsOption = for (
      stgSrcViewWithMonikerName <- if (stgSrcViewsWithMonikerNames.isEmpty) List(ModelObject.viewNameForNonExistentDataSource) else stgSrcViewsWithMonikerNames;
      dfSrcDimAsOption = if (effDateYYYY_MM_DD == null) loadDim(stgSrcViewWithMonikerName) else loadDim(effDateYYYY_MM_DD, stgSrcViewWithMonikerName);
      dfSrcDimWithEtlColsAsOption = if (dfSrcDimAsOption.isDefined) {
        val dfSrcDim = dfSrcDimAsOption.get
        val dfSrcDimWithEtlCols = {

          // This will throw an exception if there are duplicates
          checkLoadResultForDuplicateColumns( dfSrcDim, stgSrcViewWithMonikerName)

          val dimensionHasSurrogateKey = configDwEtl.getIsDimensionKeySurrogate(dimName)

          val (expectedSchema, expectedSchemaHasDimensionKeyColumn): (StructType, Boolean) =
            if (dimensionHasSurrogateKey) {
              // Require the select to exclude surrogate key. Surrogate key will be ignored if it is included anyway,
              // so to make it more structured just disallow surrogate key from the result.
              (dimSchemaWithoutKey, false)
            }
            else {
              (dimSchema, true)
            }

          checkForMismatchedFieldNamesInActualAndExpectedSchemas( dfSrcDim, stgSrcViewWithMonikerName, expectedSchema, expectedSchemaHasDimensionKeyColumn )

          // At this point the number of columns is correct and the names match the ones in the configuration.
          // The fields may still be in incorrect order.
          // Before arranging columns in correct order make sure that the types in the source result are compatible with types in the schema
          // Make sure that the fields in the result match expected types
          checkFieldsWithSameNamesForTypeCompatibility(dfSrcDim, expectedSchema, stgSrcViewWithMonikerName )

          // At this point the columns names match and types match or compatible the ones in the configuration.
          // Add dimension key if needed


          // I need the dimension key to have value of null if it is a surrogate key
          // so the key that is not set can be differentiated from the ones that are set.
          // If dimension key is not a surrogate key, then it will be populated.
          // (Only surrogate key here will be null if dimension has surrogate key)
          val dfSrcDimWithSurrogateKeyNull = if (configDwEtl.getIsDimensionKeySurrogate(dimName)) {
            dfSrcDim.withColumn(dimKey,
                lit(null).cast(configDwEtl.getDimensionKeyColType(dimName)))
          }
          else {
            // Dimension key is not surrogate
            dfSrcDim
          }

          // Confirm that dimension has all columns including the key
          checkForMismatchedFieldNamesInActualAndExpectedSchemas( dfSrcDimWithSurrogateKeyNull, stgSrcViewWithMonikerName, dimSchema, true )

          if (isDebugDwLib) {
            dfSrcDimWithSurrogateKeyNull.printSchema()
          }

          val dfWithEtlCols = addEtlColumns(
            dfSrcDimWithSurrogateKeyNull.selectInCorrectOrderCastToCorrectTypeAndMakeNullable( dimSchema ), // at this point the dataframe will have all columns incl. the key
            stgSrcViewWithMonikerName,
            effDateYYYY_MM_DD)

          // Remove a row, if exists, where natural key(s) are same as for Unknown row. It is already part of a dimension.
          dfWithEtlCols.where(s"""CompoundKey != '${configDwEtl.getDimensionSurrogateKeyForUnknownRow(dimName)}' """)
        }
        Some(dfSrcDimWithEtlCols)
      }
      else {
        None
      }

      if (dfSrcDimWithEtlColsAsOption.isDefined) // filter out all empty sources
    ) yield dfSrcDimWithEtlColsAsOption

    if (isDebugDwLib) {
      if (!dfDimsFromAllSourcesAsOption.isEmpty) {
        dfDimsFromAllSourcesAsOption.head.getOrElse(dfEmptyStgSrcDim).printSchema()
        dfDimsFromAllSourcesAsOption.head.getOrElse(dfEmptyStgSrcDim).count()
      }
    }

    // Merge the results from each source giving preference to the ones earlier in the list
    val dfSrcDimAsOption = if (dfDimsFromAllSourcesAsOption.isEmpty) {
      None
    }
    else if (dfDimsFromAllSourcesAsOption.length == 1) {
      dfDimsFromAllSourcesAsOption.head
    }
    else {
      dfDimsFromAllSourcesAsOption.reduceLeft((df1AsOption, df2AsOption) =>
        if (df1AsOption.isDefined && df2AsOption.isDefined) {
          Some(df1AsOption.get.union(
            df2AsOption.get.join(df1AsOption.get, Seq("CompoundKey"), "leftanti")
              .select(dimColsInclEtlOnes.head, dimColsInclEtlOnes.tail: _*) // leftanti moves the CompoundKey to the front - so select columns in correct order
          ))
        }
        else if (df1AsOption.isDefined && !df2AsOption.isDefined) {
          df1AsOption
        }
        else if (!df1AsOption.isDefined && df2AsOption.isDefined) {
          df2AsOption
        }
        else {
          None
        }
      )
    }

    if (isDebugDwLib) {
      dfSrcDimAsOption.getOrElse(dfEmptyStgSrcDim).show(5);
      dfSrcDimAsOption.getOrElse(dfEmptyStgSrcDim).printSchema()
    }

    val dfEnrichedSrcDimAsOption = if (dfSrcDimAsOption.isDefined)
      Some(enrichDim(dfSrcDimAsOption.get, if (configDwEtl.getIsInitialLoad) None else Some(viewDimExisting)))
    else
      None

    if (isDebugDwLib) {
      dfEnrichedSrcDimAsOption.getOrElse(dfEmptyStgSrcDim).show(5);
      dfEnrichedSrcDimAsOption.getOrElse(dfEmptyStgSrcDim).printSchema()
    }
    dfEnrichedSrcDimAsOption
  }

/*
  private def createErrorMessageForMismatchedFieldsInActualAndExpectedSchemas(stgSrcViewWithMonikerName: String, actualSchema: StructType, expectedSchema: StructType, expectedSchemaHasDimensionKeyColumn: Boolean): String = {
    val baseErrorMessage = if (expectedSchema.length == actualSchema.length)
      s"""Etl Runner ERROR: The following discrepancies in column names for \"$dimName\" dimension may be the reason for the error: """
    else
      s"""Etl Runner ERROR: Data for \"$dimName\" dimension ${if (stgSrcViewWithMonikerName == ModelObject.viewNameForNonExistentDataSource) "" else "from \"" + stgSrcViewWithMonikerName + "\" source "}has incorrect number of columns - expected ${expectedSchema.length} ${if (!expectedSchemaHasDimensionKeyColumn) "(that excludes surrogate key which must not be included in the load result) " else ""}vs. actual ${actualSchema.length} """

    // if there is at least one field with the same name, assume the intention is to have matching names and in that case determine fields missing from either schema
    val commonFields = actualSchema.names.intersect(expectedSchema.names)
    val errorMessage: String = if (commonFields.length > 0) {
      val fieldsMissingFromLoadedDimension = expectedSchema.names.diff(actualSchema.names).mkString(", ")
      val fieldsMissingFromConfiguration = actualSchema.names.diff(expectedSchema.names).mkString(", ")
      (if (!fieldsMissingFromLoadedDimension.isEmpty)
        s"""\nThe following fields defined in the configuration are missing from the dimension data: $fieldsMissingFromLoadedDimension """
      else
        "") +
        (if (!fieldsMissingFromConfiguration.isEmpty)
          s"""\nThe following fields defined in the dimension data do not exist in the configuration${if (!expectedSchemaHasDimensionKeyColumn) " (or is dimension key that should not be included) " else ""}: $fieldsMissingFromConfiguration """
        else
          "")
    }
    else
      "\nNone of the fields' names in the result match field names in dimension configuration. Use field alias to associate columns in the result with columns in the dimension configuration."

    if (expectedSchema.length == actualSchema.length && errorMessage.isEmpty)
      ""
    else
      baseErrorMessage + errorMessage
  }
*/

  /**
   *
   * @param dfDimNewWithSurrogKeyNull - some keys in this dim are set and some are null. The ones that are set do not change
   *                                  and the ones that are null are being set
   * @param startSurrogateKeyValue
   * @return
   */
  private def setSurrogateKeyValue(
      dfDimNewWithSurrogKeyNull: DataFrame,
      startSurrogateKeyValue: Long): DataFrame = {

    val dfWithSurrogateKeySet = dfDimNewWithSurrogKeyNull
      .withColumn(dimKey, monotonically_increasing_id())
      // to allow for dimension key growth shift the partition part of monotonically increasing id value by 4 positions
      // assuming that the number of rows in a single partition will not exceed 500 million
      .withColumn(dimKey,
        {
          val value =col(dimKey) % 8589934592L + (col(dimKey) / lit(8589934592L)).cast(LongType) * lit(536870912L) + lit(startSurrogateKeyValue);
          when(value.isNotNull, value).otherwise(lit(null))
        }
      )

    if (isDebugDwLib) {
      dfWithSurrogateKeySet.printSchema()
      dfWithSurrogateKeySet.show(40)
    }

    dfWithSurrogateKeySet
  }

  private def saveDim(dfDim: DataFrame): Unit = {

    if (!configDwEtl.getIsDimensionsDestinationParquet) {
      throw new RuntimeException("""Dim Etl ERROR: Only Parquet dimension destination is currently supported """)
    }

    if (isDebugDwLib) {
      dfDim.printSchema()
      dfDim.show(5)
      dwEtlLog.info("Saved new version of dimension " + dimName + " with row count: " + dfDim.count().toString)
    }

    if (configDwEtl.getIsSavePreviousDimVersionOfDestinationFile) {
      FileHelper.saveDataFrameAsParquet(
        dfDim,
        configDwEtl.getDimensionFilePath(dimName),
        configDwEtl.getDimensionPreviousVersionFilePath(dimName))
    }
    else {
      FileHelper.saveDataFrameAsParquet(dfDim, configDwEtl.getDimensionFilePath(dimName))
    }
  }

  /**
   * Gets the existing dimension or creates it with Unknown row
   */
  private def getOrCreateDimension(): DataFrame = {

    // On initial load create dimension with unknown row
    if (isInitialLoad) {
      // Create new df and populate with Unknown row
      val colUnknownValues: ListMap[String, String] = configDwEtl.getDimensionColNamesAndUnknownValues(dimName)

      // Create Seq with unknown values.
      // Populate null for unspecified values except for dimension Key - set that to zero if not defined
      val unknownValues: Seq[Any] = dimSchema.map {
        case StructField(col, ShortType, _, _) =>
          if (colUnknownValues.contains(col)) colUnknownValues(col).toShort
          else if (configDwEtl.getDimensionKeyCol(dimName) == col) 0 // default for Unknown value for key is zero regardless if it is surrogate or not
          else null // null works for integers, not only for objects
        case StructField(col, IntegerType, _, _) =>
          if (colUnknownValues.contains(col)) colUnknownValues(col).toInt
          else if (configDwEtl.getDimensionKeyCol(dimName) == col) 0 // default for Unknown value for key is zero regardless if it is surrogate or not
          else null // null works for integers, not only for objects
        case StructField(col, LongType, _, _) =>
          if (colUnknownValues.contains(col)) colUnknownValues(col).toLong
          else if (configDwEtl.getDimensionKeyCol(dimName) == col) 0L // default for Unknown value for key is zero regardless if it is surrogate or not
          else null // null works for integers, not only for objects
        case StructField(col, DateType, _, _) =>
          if (colUnknownValues.contains(col)) SqlDate.valueOf(colUnknownValues(col)) else null
        case StructField(col, TimestampType, _, _) =>
          if (colUnknownValues.contains(col)) SqlTimestamp.valueOf(colUnknownValues(col)) else null
        case StructField(col, StringType, _, _) =>
          if (colUnknownValues.contains(col)) colUnknownValues(col) else null
        case StructField(col, DoubleType, _, _) =>
          if (colUnknownValues.contains(col)) colUnknownValues(col).toDouble else null

        case y: StructField => throw new RuntimeException("""dwEtl ERROR: unsupported dimension data type""")
      }

      val unknownRow = Seq(Row.fromSeq(unknownValues))
      val unknownRDD = sparkModelObject.sparkContext.parallelize(unknownRow)
      val dfDim = sparkModelObject.createDataFrame(unknownRDD, dimSchema)
      addEtlColumns(dfDim)
    }
    else {
      if (configDwEtl.getIsDimensionsDestinationParquet) {
        val dimFilePath = configDwEtl.getDimensionFilePath(dimName)
        sparkModelObject.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
          .load(dimFilePath)
      }
      else {
        throw new RuntimeException("""dwEtl ERROR: only Parquet file are valid dimension destination""")
      }
    }
  }

  private def addEtlColumns(
      dfDim: DataFrame,
      sourceSystem: String = "N/A",
      loadEffDateYYYY_MM_DD: String = farPastDateYYYY_MM_DD,
      startDateYYYY_MM_DD: String = farPastDateYYYY_MM_DD,
      endDateYYYY_MM_DD: String = farFutureDateYYYY_MM_DD,
      mostRecentIndicator: Boolean = true): DataFrame = {

    if (isDebugDwLib) {
      dfDim.printSchema()
    }

    val dfDimWithEtlCols = dfDim.withColumns(ListMap(
      "CompoundKey" -> {
        val value = concatColumns(struct(naturalKeys.head, naturalKeys.tail: _*)).cast(StringType);
        when(value.isNotNull, value).otherwise(lit(null))
      },
      "TypeOneColumnsHash" -> {
        val value = if (!hasTypeOneCols) lit("").cast(StringType) else md5(concatColumns(struct(typeOneCols.head, typeOneCols.tail: _*)));
        when(value.isNotNull, value).otherwise(lit(null))
      },
      "TypeTwoColumnsHash" -> {
        val value = if (!hasTypeTwoCols) lit("").cast(StringType) else md5(concatColumns(struct(typeTwoCols.head, typeTwoCols.tail: _*)));
        when(value.isNotNull, value).otherwise(lit(null))
      },
      "SourceSystem" -> {
        val value = lit(sourceSystem).cast(StringType);
        when(value.isNotNull, value).otherwise(lit(null))
      },
      "LoadEffectiveDate" -> {
        val value = to_date(lit(loadEffDateYYYY_MM_DD));
        when(value.isNotNull, value).otherwise(lit(null))
      },
      "CreatedOn" -> when((current_timestamp()).isNotNull, current_timestamp()).otherwise(lit(null)),
      "CreatedBy" -> {
        val value = lit(System.getProperty("user.name"));
        when(value.isNotNull, value).otherwise(lit(null))
      },
      "StartDate" -> {
        val value = to_date(lit(startDateYYYY_MM_DD));
        when(value.isNotNull, value).otherwise(lit(null))
      },
      "EndDate" -> {
        val value = to_date(lit(endDateYYYY_MM_DD));
        when(value.isNotNull, value).otherwise(lit(null))
      },
      "MostRecentIndicator" -> {
        val value = lit(mostRecentIndicator);
        when(value.isNotNull, value).otherwise(lit(null))
      },
      "UpdatedOn" -> {
        val value = current_timestamp();
        when(value.isNotNull, value).otherwise(lit(null))
      },
      "UpdatedBy" -> {
        val value = lit(System.getProperty("user.name"));
        when(value.isNotNull, value).otherwise(lit(null))
      }
    ))

    if (isDebugDwLib) {
      dfDimWithEtlCols.printSchema()
      dfDimWithEtlCols.show(5)
    }

    dfDimWithEtlCols.setNullableStateForAllColumns(true)
  }

  /**
   * This method must be overridden if the logic of setting the key is anything other than straight
   * equi-join on all Natural Keys.
   * The new key will be appended to the existing  fact Data Frame
   *
   * Setting the key in steps
   * ------------------------
   * For some dimensions the key must be set in a cascading fashion based on some precedence.
   * The steps for this in the override are following:
   * - in step 1 create new dataFrame with the key either set using the first rule or NULL
   * - in the next step create a new DF for step 2 using the df from step 1 as input
   * where the new key is NULL and replace the last column with the non-NULL key based
   * on the logic for this step. The resulting dataFrame may still have some keys NULL
   * - continue with more steps like above
   * - in the last step repeat the same logic using the df from the previous step as input, only
   * in this case set the key that cannot be set to Unknown value instead of NULL.
   * - union all dataFrames from all steps where the key is not NULL
   * - the final dataFrame returned should have one more column with a new key with all keys non-NULL
   *
   * @param fact                 - the data frame to set the key on.
   * @param mapDimColsToFactCols - this map includes only columns explicitly defined in fact schema.
   *                             That is this map does not include the Start/End dates for type 2 dimension ,
   *                             the CompoundKey column, or the key itself
   *                             For this Map the order in not important as the columns can be
   *                             listed in the join in any order, so do not need ListMap for it.
   * @param effDateColAsOption   - effective date will be used for type 2 dimensions
   * @return - a new dataFrame with one more column - a key for this dimension. The name of the new
   *         column does not matter - it will be set to the one in schema in further processing
   */
  // Need setForeignKeyOnFactTable that is protected, so can be used in derived dimension for specific implementation,
  // but also visible from the package from another class, so protected[etl] is for that
  protected[etl] def setForeignKeyOnFactTable(fact: DataFrame, mapDimColsToFactCols: Map[String, String], effDateColAsOption: Option[String]): DataFrame = {
    dwEtlLog.info(s""" -- Setting keys on fact table from dimension $dimName using natural keys only """)

    if (isDebugDwLib) {
      FileHelper.saveDataFrameAsParquet(fact, configDwEtl.getFactFilePath("DebugForeignKeys") + s".1.$dimKey")
    }

    if (!effDateColAsOption.isDefined && hasTypeTwoCols) {
      throw new RuntimeException(s"""ETL  ERROR: The schema for fact table that uses type 2 dimension $dimName must have a date column with isEffDateForTypeTwo attribute set to true.""")
    }

    val viewFactTable = s"factWhenSettingKeyFrom$dimName"
    fact.createOrReplaceTempView(viewFactTable)

    val argsDimCols = mapDimColsToFactCols.keys.toList
    val naturalKeys = configDwEtl.getDimNaturalKeys(dimName)

    // Compare both lists. Because the elements can be in different order compare them by diffing
    if (!argsDimCols.diff(naturalKeys).isEmpty || !naturalKeys.diff(argsDimCols).isEmpty) {
      throw new RuntimeException(s"""ETL  ERROR: setForeignKeyOnFactTable method must be overridden in the dimension class for $dimName dimension. The columns to set keys do not include all natural keys only.""")
    }

    val joinConditions = mapDimColsToFactCols.map {
      case (dimCol, factCol) => s"dim.$dimCol <=> fact.$factCol\n"
    }

    //  There is a view created with the same name as dimension
    // We use NULL safe join as if both columns are NULLs this is a good key
    val sqlToSetKey =
      s"""
         |SELECT fact.*, IFNULL( dim.$dimKey, $dimKeyUnknownValue )
         |FROM $viewFactTable AS fact
         |  LEFT OUTER JOIN $dimName AS dim ON ${joinConditions.mkString("    ", "AND ", "")}
         |                              ${if (hasTypeTwoCols) s"AND fact.${effDateColAsOption.get} BETWEEN dim.StartDate AND dim.EndDate" else ""}
         |""".stripMargin

    dwEtlLog.info(s"-- Setting fact key using sql:\n$sqlToSetKey")

    val dfFactWithNewKey = sparkModelObject.sql(sqlToSetKey)

    if (isDebugDwLib) {
      dfFactWithNewKey.printSchema()
      dfFactWithNewKey.show(10)
    }

    dfFactWithNewKey
  }

}
