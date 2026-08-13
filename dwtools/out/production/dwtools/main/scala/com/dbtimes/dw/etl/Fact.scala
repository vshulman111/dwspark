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

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.dbtimes.dw.common.SourceDataMerger
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions.{col, current_timestamp, lit}
import org.apache.spark.sql.DataFrame

import com.dbtimes.dw.common.DataFrameHelper.DataFrameImplicits
import com.dbtimes.dw.common.FileHelper
import com.dbtimes.dw.common.MiscHelper.getDateFormatted
import com.dbtimes.dw.common.LogFile.{logger => dwEtlLog}
import com.dbtimes.dw.common.SourceDataAction

private[etl] object Fact {

  private var dataMartPackageName = ""

  private[etl] def setDataMartPackageName(dataMartPackageName: String): Unit = {
    Fact.dataMartPackageName = dataMartPackageName
  }

  private def getFact(factName: String): Fact = {

    val constructor = Class.forName(dataMartPackageName + "." + factName).getDeclaredConstructor(classOf[String])
    val fact = constructor.newInstance(factName).asInstanceOf[Fact]
    fact
  }

  /**
   *
   * @param mapDimNameDf - dimension processed as part of this load. These dimensions were loaded by the same ETL that is loading fact tables
   * @param configDwEtl
   */
  private[etl] def etlAllFacts(mapDimNameDf: Map[String, DataFrame], configDwEtl: ConfigDwEtl): Unit = {

    // loop trough all configurations and perform the loads
    val factNames: List[String] = configDwEtl.getFactNamesToLoad
    factNames.foreach(Fact.etlFact(_)(mapDimNameDf, configDwEtl)) // foreach expects a function with one parameter, so using second parameter list
  }

  private def etlFact(factName: String)(mapDimNameDf: Map[String, DataFrame], configDwEtl: ConfigDwEtl): Unit = {
    dwEtlLog.info("---- Processing fact table: " + factName)

    val timeStart = Calendar.getInstance().getTimeInMillis()
    val fact = Fact.getFact(factName)
    val stgSourceTempViews: List[String] = fact.loadStagingSources()
    val datesToProcess: List[Date] = fact.getEffectiveDatesToProcess()

    if (datesToProcess.isEmpty) {
      dwEtlLog.info(s"-- There are no dates to process fact table $factName")
    }
    else {
      fact.loadDimensions(mapDimNameDf)
      fact.etlModelObject(stgSourceTempViews, datesToProcess)
    }
    val durationSec = (Calendar.getInstance().getTimeInMillis() - timeStart).toDouble / 1e3 // seconds

    // For the Etl Process record the last effective date from the shared dates to process because that is
    // created from the sources marked for effective date and that's what will be used to get a new
    // dates range for the incremental load
    ModelObject.createEtlLogRecord(factName, configDwEtl, configDwEtl.getStgSourceMonikersOfFact(factName), datesToProcess,
      configDwEtl.getIsInitialLoad, if (configDwEtl.getRerunEtlAfter.isDefined) true else false, durationSec)
  }

}

/**
 *
 * @param factName
 */

abstract class Fact(protected val factName: String)
  extends ModelObject(factName)
    with SourceDataAction
    with SourceDataMerger {

  private val factSchema = configDwEtl.getFactSchema(factName)

  /** Overridable methods
   * Notes: this is the code snippet to get last loaded timestamp in the loadDim if needed.
   * The second line would convert it to string if needed to be used inside SQL statement
   * val stgSrcLastLoadedTimestampAsOption = lastProcessedStgSourceTimestamp.getOrElse( stgSrcTempView, None );
   * val stgSrcLastLoadedTimestampAsStringAsOption = if ( stgSrcLastLoadedTimestampAsOption.isDefined ) Some( stgSrcLastLoadedTimestampAsOption.get.toString ) else None;
   */
  protected def loadFact(stgSrcTempView: String): Option[DataFrame] = None // Override this method to load new data in bulk or for all effective dates. In this override, you would normally join to dates view to generate effective date value
  protected def loadFact(effDateYYYY_MM_DD: String, stgSrcTempView: String): Option[DataFrame] = None // Override this method to load new data one effective dates at a time. Dimensions with type 2 columns can only use this method to load new data
  // End of overridable methods

  final override protected def etlModelObject(stgSourceTempViews: List[String], datesToProcess: List[Date]): Option[DataFrame] = {

    dwEtlLog.info(s"-- Processing fact $factName for following ${datesToProcess.size} dates: ${datesToProcess.mkString(",")}")

    // Delete existing fact tables on initial load, if exists
    if (isInitialLoad) {
      FileHelper.deleteDirectoryOrFileIfExists(configDwEtl.getFactFilePath(factName, false))
    }

    preProcess(stgSourceTempViews, datesToProcess)

    val dfFactForAllEffectiveDatesAtOnceAsOption = doLoadForAllSources(stgSourceTempViews)

    val dfFactForAllEffectiveDatesOneAtATimeAsOption = if (!dfFactForAllEffectiveDatesAtOnceAsOption.isDefined) {
      val dfFactForEachEffectiveDateAsOption = for (effDate <- datesToProcess;
                                                    dfFactForEffectiveDateAsOption = doLoadForAllSources(stgSourceTempViews, getDateFormatted(effDate, "yyyy-MM-dd")) // effDate.toString
                                                    ) yield dfFactForEffectiveDateAsOption
      // merge all dataframes into one for all effective dates
      val dfFactForAllEffectiveDatesAsOption = dfFactForEachEffectiveDateAsOption.reduceLeft((df1AsOption, df2AsOption) =>
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

      // The value of this Option can be None if none of the sources were used to extract fact table
      // This can happen, for example, when the processing is MERGE and data is loded only for merge keys that changed
      dfFactForAllEffectiveDatesAsOption
    }
    else {
      None
    }


    if (dfFactForAllEffectiveDatesAtOnceAsOption.isDefined || dfFactForAllEffectiveDatesOneAtATimeAsOption.isDefined) {

      val dfFactForAllEffectiveDates = dfFactForAllEffectiveDatesAtOnceAsOption.getOrElse(dfFactForAllEffectiveDatesOneAtATimeAsOption.get)

      // Set Keys on combined facts from all sources
      dwEtlLog.info(s"-- Setting keys on fact $factName for all dates and all sources")
      val dfFactWithKeys = setKeys(dfFactForAllEffectiveDates)

      val dfFactWithKeysPostProcessed = postProcess(stgSourceTempViews, datesToProcess, dfFactWithKeys)

      // Persist fact table. It will be used to create a copy with values to set keys as well as the fact table itself
      dfFactWithKeysPostProcessed.persist(StorageLevel.MEMORY_AND_DISK)

      // This copy has the key setting columns - will be used to report on unknown
      saveFactWithKeySetters(dfFactWithKeysPostProcessed)

      val dfFactWithKeysAndValuesOnly = createFactWithKeysAndValuesOnly(dfFactWithKeysPostProcessed)

      // Load merge key only if they are defined in schema and the fact processing is merge,
      // Other wise they are not needed
      val factProcessMode = configDwEtl.getFactProcessMode(factName)
      val dfAllMergeKeysAsOption = if (!configDwEtl.getMergeKeysCols(factName).isEmpty && List(FactProcessMode.MERGE, FactProcessMode.MERGE_PARTITION).contains(factProcessMode)) {
        val factMergeKeysCols = configDwEtl.getMergeKeysCols(factName)
        Some(dfFactWithKeysPostProcessed.select(factMergeKeysCols.head, factMergeKeysCols.tail: _*).distinct())
      } else {
        None
      }

      saveFact(dfFactWithKeysAndValuesOnly, dfAllMergeKeysAsOption, datesToProcess)
    }
    else {
      dwEtlLog.warn(s""" -- There are no data from any of the sources for fact table $factName. The fact table will not change """)
    }

    None  // This is just to match the output type. The override in Dim actually returns a df as Option
  }

  private def createFactWithKeysAndValuesOnly(dfFactWithKeySettingCols: DataFrame): DataFrame = {

    val factPartitionByCols = configDwEtl.getPartitionByCols(factName)
    val factMergeKeysCols = configDwEtl.getMergeKeysCols(factName)
    val foreignKeys = configDwEtl.getForeignKeys(factName);
    val factMeasures = configDwEtl.getFactMeasures(factName);

    // This accounts for the fact that spark will load PartitionBy columns last when using merge to create new fact table
    val factCols = factPartitionByCols ++ factMergeKeysCols ++ foreignKeys ++ factMeasures

    val dfFactWithKeysAndValuesOnly = dfFactWithKeySettingCols
      .select(factCols.head, factCols.tail: _*)

    dfFactWithKeysAndValuesOnly
  }

  private def doLoadForAllSources(
      stgSourceTempViews: List[String],
      effDateYYYY_MM_DD: String = null): Option[DataFrame] = {

    // loop though all sources to load the facts and merge data from each source into a single result
    // Sources earlier in the list have precedence over the the ones later in the list
    val dfSrcFactFromAllSourcesAsOption = for (
      stgSourceTempView <- if (stgSourceTempViews.isEmpty) List("N/A") else stgSourceTempViews;
      dfSrcFactAsOption = if (effDateYYYY_MM_DD == null) loadFact(stgSourceTempView) else loadFact(effDateYYYY_MM_DD, stgSourceTempView);
      dfSrcFactWithCorrectColumnNamesAsOption = if (dfSrcFactAsOption.isDefined) {
        val dfSrcFact = dfSrcFactAsOption.get

        val dfSrcFactWithCorrectColumnNames = if (dfSrcFact.schema.length == factSchema.length) {
          dfSrcFact.setNewSchema(factSchema)
        }
        else {
          throw new RuntimeException(s"""Etl Runner ERROR: Fact result has incorrect number of columns.\nThe expected number of columns is ${factSchema.length} and the result has ${dfSrcFact.schema.length} """)
        }

        Some(dfSrcFactWithCorrectColumnNames)
      }
      else {
        None
      }
    ) yield dfSrcFactWithCorrectColumnNamesAsOption // Meaning that if the SQL did not have AS clause for column names or the names did match schema - here they do

    if (isDebugDwLib) {
      if (dfSrcFactFromAllSourcesAsOption(0).isDefined) {
        dfSrcFactFromAllSourcesAsOption(0).get.printSchema()
        dfSrcFactFromAllSourcesAsOption(0).get.count()
      }
    }

    // Combine the results from each source giving preference to the ones earlier in the list
    val dfSrcFactAsOption = dfSrcFactFromAllSourcesAsOption.reduceLeft((df1AsOption, df2AsOption) =>
      if (df1AsOption.isDefined && df2AsOption.isDefined) {
        Some(df1AsOption.get.union(df2AsOption.get))
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

    dfSrcFactAsOption
  }

  /**
   * This function sets all keys on a fact table. Fact table keys are being set iteratively by being
   * appended to the fact table on each iteration
   *
   * In a first step it builds a list of 4-Tuple with the fields needed to set the key
   *  - first tuple member is a list of tuples for Natural Keys in the dimension
   *    ( Name of dimension Natural Key, Name of the field in the fact table with the value)
   *  - second tuple member is a list of tuples for not Natural Keys columns used to set the dimension key.
   *    In the case when there are keys like that, the dimension must provide a custom override to set a key
   *    ( Name of dimension field which is not a Natural Key, Name of the field in the fact table with the value)
   *
   * @param dfSrcFact
   * @return
   */
  private def setKeys(dfSrcFact: DataFrame): DataFrame = {

    // The paramsToSetKey is a list of Tuples with four elements.
    // 1. name of the key to set
    // 2. map of dimension columns with corresponding fact columns
    // 3. dimension to set the key
    // 4. fact table to set the key on
    //
    // In this list only the first element has Option[DataFrame] with Some( dfSrcFact), all other elements have this value set to None (see the expression "indexOf(key) == 0" to set the value only on the first elelemt)
    // The logic here is to set the keys in order, so the same source will be used to set the first key, then the second key and so forth,
    //  so we need to have a value for fact df (last member of the tuple) set to Some only for the first key and it will be reused in order for setting keys after the previous key was set.
    // The set keys are added the end of the fact data frame on each iteration of setting the key.
    // This structure will be used to populate keys using foldLeft operation below
    // The second element in the tuple is a Map - it is for pairs of dim column names to fact column names for setting the key on the fact table.
    // The order of the columns in the Map is not important, so do not need ListMap
    val paramsToSetKey: List[(String, Map[String, String], Dim, Option[DataFrame])] = for (
      key <- configDwEtl.getForeignKeys(factName);
      dimName = configDwEtl.getDimensionForSettingForeignKey(factName, key);
      mapDimColsToFactCols = configDwEtl.getMapDimColsToFactCols(factName, key);
      dim = Dim.getDim(dimName);
      dfFactAsOption = if (configDwEtl.getForeignKeys(factName).indexOf(key) == 0) Some(dfSrcFact) else None // if index is zero , i.e., this is the first key to be set, set the value of fact df, otherwise set to None
    ) yield (key, mapDimColsToFactCols, dim, dfFactAsOption)

    // Start value has to be of the same type as elements of the List for fold.
    // For start value we need the fact table - the fourth element in a tuple - with no keys set.
    // The other three elements of the start value are not used.

    val (_,_,dimNameOfFirstForeignKey, dfFactAsOptionBeforeSettingKeys) = paramsToSetKey.head
    val startValue = (
      "",
      Map[String,String] (),
      dimNameOfFirstForeignKey, // it is not easy to create an empty instance of the Dim class, so just use the the first dimension - it does not really matter - it will not be used
      dfFactAsOptionBeforeSettingKeys // This is the only value we really need in this start value
    )

    // In the very first iteration the elementTwo is the first element of the list - elementTwo - will be used together with the start value - elementOne.
    // The second iteration of the fold left, it will use the previous result, where the last tuple element will have the fact table with the firdst key.
    val resultOfSettingKeysOnFact = paramsToSetKey
      .foldLeft(startValue)((prev, curr) => {
        val (_, _, dimNameOfPreviousIteration, dfFactAsOptionWithKeysSetSoFar) = prev
        val (keyBeingSet, mapDimColumnsToFactColumns, dimNameForKeyBeingSet, _) = curr
        ("", // \
          Map[String, String](), //  | -- For the fold these members of the prev are not used to set the key.
          dimNameOfPreviousIteration, // /
          { // here we set the last member of the tuple which is the dfFact with the key for the curr tuple
            val dfFactWithNewKey = dimNameForKeyBeingSet.setForeignKeyOnFactTable( // dimNameForKeyBeingSet is an instance of the Dim
              dfFactAsOptionWithKeysSetSoFar.get, // Fact Data Frame
              mapDimColumnsToFactColumns, //Map of dimension column names and corresponding fact names needed to set a foreign key on a fact table
              configDwEtl.getEffectiveDateColNameForSettingTypeTwoKeys(factName))

            // Set correct name and the type of the key
            val dfFactWithCorrectKeyNameAndType = dfFactWithNewKey
              .withColumnRenamed(dfFactWithNewKey.schema.fieldNames.last, keyBeingSet) // keyBeingSet is the name of the ForeignKey
              .withColumn(keyBeingSet, col(keyBeingSet).cast(configDwEtl.getDimensionKeyColType(dimNameForKeyBeingSet.getName)))
              .setNullableStateForAllColumns(true)

            if (isDebugDwLib) {
              dfFactWithCorrectKeyNameAndType.printSchema()
              dfFactWithCorrectKeyNameAndType.show(10)
              val fileName = configDwEtl.getFactFilePath("DebugForeignKeys") + s".3.${keyBeingSet}"
              FileHelper.saveDataFrameAsParquet(dfFactWithCorrectKeyNameAndType, fileName)
            }

            Some(dfFactWithCorrectKeyNameAndType)
          })
      }) // a map of fact columns to corresponding dimension columns
    val (_, _, _, dfFactAsOptionWithAllKeysSet) = resultOfSettingKeysOnFact

    if (isDebugDwLib) {
      dfFactAsOptionWithAllKeysSet.get.printSchema()
      dfFactAsOptionWithAllKeysSet.get.show(10)
    }

    dfFactAsOptionWithAllKeysSet.get
  }

  private def loadDimensions(mapDimNameDf: Map[String, DataFrame]): Unit = {

    val list = for (
      dimName <- configDwEtl.getDimensionsForSettingForeignKeysOrLoadingFact(factName);
      dfDimForSettingForeignKeysOrLoadingFact = ModelObject.loadDimensionForSettingForeignKeysOrLoadingFact(dimName, mapDimNameDf, configDwEtl)
    ) yield dimName -> dfDimForSettingForeignKeysOrLoadingFact

    for ((dimName, dfDimForSettingForeignKeysOrLoadingFact) <- list) {
      dfDimForSettingForeignKeysOrLoadingFact.createOrReplaceTempView(dimName) // need this as view to set keys
    }
  }

  private def addEtlColumns(dfFact: DataFrame): DataFrame = {

    if (isDebugDwLib) {
      dfFact.printSchema()
    }

    dfFact
      .withColumn("CreatedOn", current_timestamp())
      .withColumn("CreatedBy", lit(System.getProperty("user.name")))
      .setNullableStateForAllColumns(true)
  }

  private def saveFactWithKeySetters(dfFact: DataFrame): Unit = {

    if (!configDwEtl.getIsFactsDestinationParquet) {
      throw new RuntimeException("""Etl ERROR: Only Parquet facts destination is currently supported """)
    }
    val factFilePath = configDwEtl.getFactFilePath(factName, isForKeySetters = true)

    if (isDebugDwLib) {
      dfFact.printSchema()
      dfFact.show(5)
      dwEtlLog.info("Saved new version of fact " + factName + " with row count: " + dfFact.count().toString)
    }

    FileHelper.saveDataFrameAsParquet(dfFact, factFilePath, columnsPartitionBy = configDwEtl.getPartitionByCols(factName))
  }

  private def saveFact(dfFact: DataFrame, dfAllMergeKeysAsOption: Option[DataFrame], datesToProcess: List[Date]): Unit = {

    if (!configDwEtl.getIsFactsDestinationParquet) {
      throw new RuntimeException("""Etl ERROR: Only Parquet facts destination is currently supported """)
    }
    val factFilePath = configDwEtl.getFactFilePath(factName, isForKeySetters = false)

    if (isDebugDwLib) {
      dfFact.printSchema()
      dfFact.show(5)
      dwEtlLog.info("Saved new version of fact " + factName + " with row count: " + dfFact.count().toString)
    }

    val factProcessMode = configDwEtl.getFactProcessMode(factName)

    // Add ETL Columns for all modes except Merge. During Merge the process will add merge specific
    // Etl columns used later to merge incrementally
    factProcessMode match {
      case FactProcessMode.REPLACE => SaveFactReplace(addEtlColumns(dfFact), factFilePath)
      case FactProcessMode.REPLACE_PARTITION => SaveFactReplacePartition(addEtlColumns(dfFact), factFilePath)
      case FactProcessMode.MERGE => SaveFactMerge(dfFact, dfAllMergeKeysAsOption, factFilePath, datesToProcess)
      case FactProcessMode.MERGE_PARTITION => SaveFactMergePartition(dfFact, dfAllMergeKeysAsOption, factFilePath, datesToProcess)
      case FactProcessMode.ADD => SaveFactAdd(addEtlColumns(dfFact), factFilePath)
      case _ => throw new RuntimeException("""Etl ERROR: Unknown FactProcessMode """)
    }

  }

  private def SaveFactReplace(dfFact: DataFrame, factFilePath: String): Unit = {
    FileHelper.saveDataFrameAsParquet(dfFact, factFilePath, columnsPartitionBy = configDwEtl.getPartitionByCols(factName))
  }

  private def SaveFactReplacePartition(dfFact: DataFrame, factFilePath: String): Unit = {
    if (configDwEtl.getPartitionByCols(factName).isEmpty) throw new RuntimeException("""Etl ERROR: REPLACE_PARTITION is invalid for fact table with no partitioning column """)

    FileHelper.saveDataFrameAsParquetReplacePartitions(dfFact, factFilePath, columnsPartitionBy = configDwEtl.getPartitionByCols(factName))
  }

  private def SaveFactAdd(dfFact: DataFrame, factFilePath: String): Unit = {
    FileHelper.saveDataFrameAddToExisting(dfFact, factFilePath, columnsPartitionBy = configDwEtl.getPartitionByCols(factName))
  }

  /**
   * FactProcessMode.MERGE can be done for partitioned or non-partitioned fact tables
   *
   * @param dfFact
   * @param dfAllMergeKeysAsOption
   * @param factFilePath
   */
  private def SaveFactMerge(dfFact: DataFrame, dfAllMergeKeysAsOption: Option[DataFrame], factFilePath: String, datesToProcess: List[Date]): Unit = {
    val dfFactWithServiceCols = addServiceColumnsAndPrepareForMerge(this, dfFact, datesToProcess.head, dwEtlLog)
    if (isInitialLoad) {
      FileHelper.saveDataFrameAsParquet(dfFactWithServiceCols, factFilePath, columnsPartitionBy = configDwEtl.getPartitionByCols(factName))
    }
    else {
      val dfOldFact = if (configDwEtl.getIsFactsDestinationParquet) {
        spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
          .load(factFilePath)
      }
      else {
        throw new RuntimeException("""dwEtl ERROR: only Parquet file are valid fact destination""")
      }

      if (!dfAllMergeKeysAsOption.isDefined) {
        throw new RuntimeException(s"""dwEtl ERROR: All merge keys DataFrame is not defined. It is needed to perform fact MERGE. Check if getFactMergeKeys method is implemented for $factName fact.""")
      }

      // if the data partitioned, the partitioned columns will be the last.
      // However the new data dfFactWitServiceCols has all columns in correct order
      val schemaWithServiceCols = dfFactWithServiceCols.schema
      val dfOldFactWithNormalizedSchema = dfOldFact.setNewSchema(schemaWithServiceCols)
      val dfFactMerged = mergeSourceDataChanges(this, dfFactWithServiceCols, true, dfAllMergeKeysAsOption.get, dfOldFactWithNormalizedSchema, new SimpleDateFormat("yyyy-MM-dd").parse("1900-01-01"), dwEtlLog)
      FileHelper.saveDataFrameAsParquet(dfFactMerged, factFilePath, columnsPartitionBy = configDwEtl.getPartitionByCols(factName))
    }
  }

  private def SaveFactMergePartition(dfFact: DataFrame, dfAllMergeKeysAsOption: Option[DataFrame], factFilePath: String, datesToProcess: List[Date]): Unit = {
    val mergeKeys = configDwEtl.getMergeKeysCols(factName)
    if (mergeKeys.isEmpty) {
      throw new RuntimeException(s"""dwEtl ERROR: No merge keys are defined for $factName fact table. Mode MERGE PARTITION requires definition of merge keys is the fact table schema.""")
    }

    val dfFactWithServiceCols = addServiceColumnsAndPrepareForMerge(this, dfFact, datesToProcess.head, dwEtlLog)

    if (isInitialLoad) {
      FileHelper.saveDataFrameAsParquet(dfFactWithServiceCols, factFilePath, columnsPartitionBy = configDwEtl.getPartitionByCols(factName))
    } else {
      if (!FileHelper.isDirectoryOrFileExists( factFilePath ) ) {
        // This is the scenario when the fact partition is loaded for the first time in parallel. We cannot specify "isInitialLoad"  on all partitions because
        // it will trigger the deletion of entire fact table, so the "isInitialLoad" in the configuration is set to false so the fact file file is not deleted.
        // The partition does not exist so there is no need to do the merge - just need to create it
        FileHelper.saveDataFrameAsParquetReplacePartitions(dfFactWithServiceCols, factFilePath, columnsPartitionBy = configDwEtl.getPartitionByCols(factName))
      } else {
        val dfOldFactForChangedPartitions = if (configDwEtl.getIsFactsDestinationParquet) {
          val dfOldFact = spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
            .load(factFilePath)

          dfOldFact.createOrReplaceTempView("OldFactData")
          dfFact.createOrReplaceTempView("ChangedFactData")
          val joinConditions = mergeKeys.map { case (col) => s"oldFact.$col = changedPartitions.$col\n" }

          val sqlGetOldChangedPartitins =
            s"""
               |SELECT oldFact.*
               |FROM OldFactData AS oldFact
               |   INNER JOIN (  SELECT ${mergeKeys.mkString("\n  ", ",\n  ", "")}
               |                 FROM ChangedFactData
               |                 GROUP BY ${mergeKeys.mkString("\n  ", ",\n  ", "")} ) AS changedPartitions  ON ${joinConditions.mkString("    ", "AND ", "")}
               | """.stripMargin

          dwEtlLog.info(s"-- Loading old fact changed partitions using sql:\n" + sqlGetOldChangedPartitins)
          spark.sql(sqlGetOldChangedPartitins)
        }
        else {
          throw new RuntimeException("""dwEtl ERROR: only Parquet file are valid fact destination""")
        }

        if (!dfAllMergeKeysAsOption.isDefined) {
          throw new RuntimeException(s"""dwEtl ERROR: All merge keys DataFrame is not defined. It is needed to perform fact MERGE PARTITION. Check if getFactMergeKeys method is implemented for $factName fact.""")
        }

        // if the data partitioned, the partitioned columns will be the last.
        // However the new data dfFactWitServiceCols has all columns in correct order
        val schemaWithServiceCols = dfFactWithServiceCols.schema
        val dfOldFactWithNormalizedSchema = dfOldFactForChangedPartitions.setNewSchema(schemaWithServiceCols)
        val dfFactMerged = mergeSourceDataChanges(this, dfFactWithServiceCols, true, dfAllMergeKeysAsOption.get, dfOldFactWithNormalizedSchema, new SimpleDateFormat("yyyy-MM-dd").parse("1900-01-01"), dwEtlLog)

        FileHelper.saveDataFrameAsParquetReplacePartitions(dfFactMerged, factFilePath, columnsPartitionBy = configDwEtl.getPartitionByCols(factName))
      }
    }

  }

  // SourceDataMerger is used for Fact table processing. Override SourceDataAction trait here to support
  // Fact table MERGE and MERGE_PARTITION processing
  override private[dw] def getSdaIsInitialLoad: Boolean = false

  override private[dw] def getSdaDestinationFilePath: String = null

  override private[dw] def getSdaPrimaryKeysList: List[String] = List.empty

  override private[dw] def getSdaExcludeFromVersioningColumnList: List[String] = List.empty

  override private[dw] def getSdaMergeKeysList: List[String] = configDwEtl.getMergeKeysCols(factName)

  override private[dw] def getSdaIsMaintainLoadControlAsParquetFile: Boolean = false

  override private[dw] def getSdaIsFileDestination: Boolean = false

  override private[dw] def getSdaIsDbmsDestination: Boolean = false

  override private[dw] def getSdaIsFileDestinationParquet: Boolean = false

  override private[dw] def getSdaIsVersioned: Boolean = false // for now. We would need versioned to implement compressed snapshotting

  override private[dw] def getSdaIsDebugDwLib: Boolean = isDebugDwLib

  override private[dw] def getSdaIsSavePreviousVersionOfDestinationFile: Boolean = false

  override private[dw] def getSdaFileDestinationSavePreviousVersionAs: String = null

  override private[dw] def getSdaSourceTypeDescription: String = "DataFrame"

  override private[dw] def getSdaSourceDescription: String = "Fact Processing DataFrame"

  override private[dw] def getSdaLoadControlParquetFileDir: String = null

  override private[dw] def getSdaIsRemoveDuplicateRows: Boolean = false

  override private[dw] def getSdaServiceColumnPrefix: String = ""

}
