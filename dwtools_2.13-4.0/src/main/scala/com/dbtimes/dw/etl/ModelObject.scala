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

import com.dbtimes.dw.common.DataFrameHelper.DataFrameImplicits
import com.dbtimes.dw.common.{DataFrameHelper, FileHelper, LogFile, MiscHelper}
import com.dbtimes.dw.common.LogFile.{dwlogger => dwEtlLog}
import com.dbtimes.dw.common.MiscHelper.getDateFormatted
import java.util.{Calendar, Date}

import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.sql.{Date => SqlDate, Timestamp => SqlTimestamp}
import java.text.SimpleDateFormat
import java.util.UUID.randomUUID

import com.dbtimes.dw.etl.EffectiveDateRule.EffectiveDateRule
import com.typesafe.config.Config
import org.apache.spark.sql.functions.max
import org.slf4j.Logger

import scala.collection.mutable
import scala.jdk.CollectionConverters._

object ModelObject {

  private[etl] val jobId = randomUUID().toString
  private[etl] val viewNameForNonExistentDataSource = "N/A"
  private val effDateColumnNameInDatesToProcess = "EffDate"
  private var stagingSources: Map[String, DataFrame] = Map.empty
  private var stagingSourcesTimestamps: Map[String, Option[SqlTimestamp]] = Map.empty
  private var configDwEtl: ConfigDwEtl = null

  def validateConfig(appConfig: Config): Unit = {

    val schemaBaseFileName = "dwetl-config-schema"
    var errors: mutable.Seq[String] = mutable.Seq.empty[String]

    // Validate schema in three steps:
    // Step 1. Validate schema version of configuration file
    // Step 2. Load schema of correct version and validate configuration against that version
    // Step 3. Validate schema in the code for components not supported by generic  schema validation
    //    - unique names of actions

    // Do Steps 1 and 2
    val errorsAndwarnings = MiscHelper.validateConfig(appConfig, schemaBaseFileName )

    errors ++= errorsAndwarnings._1
    val warnings = errorsAndwarnings._2

    // Step 3. Validate schema in the code for components not supported by generic  schema validation
    //          Step 3 depends on schema being validated in steps 1 and 2 because it assumes that specific paths and fields exist
    if (errors.isEmpty) {
      val confDwEtl = new ConfigDwEtl(appConfig)
      errors ++= confDwEtl.validateModelConfiguration()
    }

    val errorMessage = if (errors.nonEmpty) {
      errors.mkString("DW ETL Configuration ERROR(s)\n *", "\n *", "")
    }
    else
      ""
    val warningMessage = if (warnings.nonEmpty) {
      warnings.mkString(s"""${if (errorMessage.nonEmpty) "\n" else ""}DW ETL Configuration WARNINGS(s)\n *""", "\n *", "")
    }
    else
      ""

    if (errors.nonEmpty) {
      throw new RuntimeException(errorMessage + warningMessage)
    }
    else if (warnings.nonEmpty) {
      dwEtlLog.warn(warningMessage)
    }
  }

  def runEtl(appConfig: Config): Unit = {

    ModelObject.validateConfig(appConfig: Config)

    if (ModelObject.configDwEtl != null)
      throw new RuntimeException("""DW ETL ERROR: ModelObject.runEtl can only be called once in the application. """)

    ModelObject.configDwEtl = new ConfigDwEtl(appConfig)

    val spark = SparkSession.builder().getOrCreate() // this gets previously created session
    val appName = try {
      spark.sparkContext.appName
    }
    catch {
      case e: Exception => ""
    }

    dwEtlLog.info("-- Starting ETL job " + appName)

    // before loading staging sources move current dimensions to previous
    // that will allow referencing the previous dimension as a staging source
    // so some fields can be restored from previous version on initial load
    if (ModelObject.configDwEtl.getIsLoadDimensions) Dim.prepareForInitialLoad(ModelObject.configDwEtl)
    ModelObject.loadStagingSources()

    Dim.setDimAuthorityPackageName(ModelObject.configDwEtl.getDimAuthorityPackageName)
    Fact.setDataMartPackageName(ModelObject.configDwEtl.getDataMartPackageName)

    // etlAllDimensions returns map of just processed dimensions. The list can be empty if there is nothing to load on incremental load.
    // For dimensions in the list the new dimension DFs from the list will be used for setting fact keys.
    // If dimension is not in the list, it will be loaded from the file.
    // Before this logic was put into place Spark was using dimension files right away without waiting for Dim step to complete.
    // That was causing keys from new load to be set to unknown.
    val mapDimNameDfJustProcessed: Map[String, DataFrame] = if (ModelObject.configDwEtl.getIsLoadDimensions) {
      Dim.etlAllDimensions(ModelObject.configDwEtl)
    }
    else {
      Map.empty
    }

    if (ModelObject.configDwEtl.getIsLoadFacts) {
      Fact.etlAllFacts(mapDimNameDfJustProcessed, ModelObject.configDwEtl)
    }

    dwEtlLog.info("-- Completed ETL job " + appName)
  }

  private def loadStagingSources(): Unit = {

    val list = for (
      stgSourceId <- ModelObject.configDwEtl.getSourceIds;
      dfStgSource = ModelObject.loadStagingSource(stgSourceId, ModelObject.configDwEtl)
    ) yield stgSourceId -> dfStgSource

    for ((stgSourceId, dfStgSource) <- list) {
      if (isCacheStgSource(stgSourceId, ModelObject.configDwEtl)) dfStgSource.cache();
      dfStgSource.createOrReplaceTempView(stgSourceId)
    }

    ModelObject.stagingSources = list.toMap
  }

  private def loadStagingSource(sourceId: String, configDwEtl: ConfigDwEtl): DataFrame = {
    val spark = SparkSession.builder().getOrCreate() // this gets previously created session

    val dfStgSource = if (configDwEtl.getIsFileSourceParquet(sourceId)) {
      spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
        .load(configDwEtl.getSourceFilePath(sourceId))
    }
    else
      throw new RuntimeException("""ETL  ERROR: unsupported source type  """)

    if (isCacheStgSource(sourceId, configDwEtl)) dfStgSource.cache() else dfStgSource
  }

  private def getStagingSource(sourceId: String): DataFrame = {
    ModelObject.stagingSources(sourceId)
  }

  private def getStagingSourceMinChangedEffDateAndMaxTimestamp(sourceId: String, configDwEtl: ConfigDwEtl, dfLastProcessedEffectiveDateTimestampAsOption: Option[DataFrame] ): ( Option[Date], Option[SqlTimestamp] ) = {

    val spark = SparkSession.builder().getOrCreate() // this gets previously created session
    // Get max timestamp
    val maxTimestampForSourceAsOption = if (ModelObject.stagingSourcesTimestamps.contains(sourceId)) {
      ModelObject.stagingSourcesTimestamps(sourceId)
    }
    else {
      val maxRowTimestampAsOption = if (configDwEtl.getStgSourceTimestampColumn(sourceId).isDefined) {
        val maxRowTimestamp = spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
          .load(configDwEtl.getSourceFilePath(sourceId))
          .agg(max(configDwEtl.getStgSourceTimestampColumn(sourceId).get)).head().getTimestamp(0)

        Some(maxRowTimestamp)
      }
      else
        None

      ModelObject.stagingSourcesTimestamps += (sourceId -> maxRowTimestampAsOption)

      maxRowTimestampAsOption
    }

    // get effective date of the last staging load
    val changedEffDateAsOption = if ( dfLastProcessedEffectiveDateTimestampAsOption.isDefined
      && configDwEtl.getStgSourceEffDateColumn(sourceId).isDefined
      && configDwEtl.getStgSourceTimestampColumn(sourceId).isDefined) {

      val stgSourceEffDateColumn = configDwEtl.getStgSourceEffDateColumn(sourceId).get
      val stgSourceTimestampColumn = configDwEtl.getStgSourceTimestampColumn(sourceId).get

      val dfStgSource = spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
        .load(configDwEtl.getSourceFilePath(sourceId))
        .select( stgSourceEffDateColumn, stgSourceTimestampColumn )
        .distinct()

      if (configDwEtl.getIsDebugDwLib) {
        dfStgSource.show(4)
        dfStgSource.printSchema()
        dfLastProcessedEffectiveDateTimestampAsOption.get.show(4)
        dfLastProcessedEffectiveDateTimestampAsOption.get.printSchema()
      }

      import spark.implicits._
      val joinedDF = dfStgSource.as("stg")
        .join( dfLastProcessedEffectiveDateTimestampAsOption.get.as("processed"), $"stg.$stgSourceTimestampColumn" > $"processed.MaxSourceTimestamp", "inner")
        .select ( "stg.*")

      val minEffDate = joinedDF.agg( min( stgSourceEffDateColumn ).cast(DateType) ).head().getDate(0)

      if ( minEffDate == null )
        None
      else
        Some( minEffDate )
    }
    else {
      None
    }

    (changedEffDateAsOption, maxTimestampForSourceAsOption)
  }

  private def isCacheStgSource(sourceId: String, configDwEtl: ConfigDwEtl): Boolean = {
    // if (configDwEtl.getDimNamesWhereStgSourceIsUsed(sourceId).length > 1) true else false
    false // there is no simple way to determine if need to cache as the same datasets are used partially and may involve different columns
  }

  private[etl] def loadDimensionForSettingForeignKeysOrLoadingFact(dimName: String, mapDimNameDf: Map[String, DataFrame], configDwEtl: ConfigDwEtl): DataFrame = {
    val spark = SparkSession.builder().getOrCreate() // this gets previously created session
    val colsToSetKeyOrLoadingFact = Dim.getDimensionColsToSetKeyOrLoadingFact(dimName, configDwEtl)

    // Only select columns used to set key or load fact table.
    // 99% of cases it will include only Natural keys for setting the key
    // and effective dates for type 2 dimensions.
    // these partial dimension will be much smaller than full dimension.
    // It will be broadcast to all nodes
    val dfDim = if (mapDimNameDf.contains(dimName)) {
      mapDimNameDf(dimName)
    }
    else {
      spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
        .load(configDwEtl.getDimensionFilePath(dimName))
    }

    dfDim.select(colsToSetKeyOrLoadingFact.head, colsToSetKeyOrLoadingFact.tail: _*)
  }

  /**
   * Gets a configuration based effective dates
   *
   * @return DataFrame with a single column "EffDate" of DateType. "EffDate" is a well known name to be used in the code
   */
  private def getEffectiveDatesToProcessBasedOnConfig(configDwEtl: ConfigDwEtl, modelObjectName: String): DataFrame = {

    val dfEffDate = if (configDwEtl.getIsInitialLoad) {
      ModelObject.getEffectiveDatesToProcessForInitialLoadBasedOnConfig(configDwEtl, modelObjectName)
    } else {
      ModelObject.getEffectiveDatesToProcessForIncrLoadBasedOnConfig(configDwEtl, modelObjectName)
    }

    if (configDwEtl.getIsDebugDwLib) {
      dfEffDate.show(5);
      dfEffDate.printSchema()
    }

    dfEffDate.cache()
  }

  private def craeteEmptyEffeciveDatesDataFrame(): DataFrame = {
    val spark = SparkSession.builder().getOrCreate() // this gets previously created session
    spark.emptyDataFrame
      .select(lit(null).cast(DateType).as(effDateColumnNameInDatesToProcess))
  }

  /**
   *
   * @return DataFrame with a single column "EffDate" of DateType. "EffDate" is a well known name to be used in the code
   */
  private def getEffectiveDatesToProcessForInitialLoadBasedOnConfig(configDwEtl: ConfigDwEtl, modelObjectName: String): DataFrame = {
    val dfEffDate = if (configDwEtl.getStgSourceIdsForDeterminingEffectiveDates(modelObjectName).isEmpty) {
      craeteEmptyEffeciveDatesDataFrame()
    }
    else {
      val (stgSourceId, (effectiveDateColNameInStgSrc, effectiveDateRule, timestampColumnInStgSrcAsOption))
      = configDwEtl.getStgSourceIdsForDeterminingEffectiveDates(modelObjectName).head

      val dfDates = getEffectiveDatesBasedOnTheRule(effectiveDateRule, ModelObject.getStagingSource(stgSourceId), effectiveDateColNameInStgSrc)

      // Normalize the dates df by dropping specific effective date column from the source and replacing it with "well known" name "EffDate"
      dfDates.withColumn(ModelObject.effDateColumnNameInDatesToProcess, dfDates(effectiveDateColNameInStgSrc).cast(DateType))
        .drop(effectiveDateColNameInStgSrc)
    }

    if (configDwEtl.getIsDebugDwLib) {
      dfEffDate.show(5);
      dfEffDate.printSchema()
    }

    dfEffDate.cache()
  }

  private def getEffectiveDatesBasedOnTheRule(effectiveDateRule: EffectiveDateRule, stgSourceWithEffectiveDate: DataFrame, effectiveDateColNameInStgSrc: String): DataFrame = {

    val spark = SparkSession.builder().getOrCreate() // this gets previously created session
    val stgSourceWithEffectiveDateViewName = "stgSourceWithEffectiveDate" + java.util.UUID.randomUUID.toString.replace("-", "_")
    stgSourceWithEffectiveDate.createOrReplaceTempView(stgSourceWithEffectiveDateViewName)
    val sqlDatesToProcess = effectiveDateRule match {
      case EffectiveDateRule.DISTINCT_DATES => s"""
                                                  |SELECT CAST( $effectiveDateColNameInStgSrc AS DATE ) AS $effectiveDateColNameInStgSrc
                                                  |FROM $stgSourceWithEffectiveDateViewName
                                                  |WHERE $effectiveDateColNameInStgSrc IS NOT NULL
                                                  |GROUP BY CAST( $effectiveDateColNameInStgSrc AS DATE )
                                                  |HAVING $effectiveDateColNameInStgSrc IS NOT NULL
                                                  |""".stripMargin
      case EffectiveDateRule.ALL_DATES => s"""
                                             |SELECT EXPLODE(SEQUENCE(MIN( CAST( $effectiveDateColNameInStgSrc AS DATE ) ), MAX( CAST( $effectiveDateColNameInStgSrc AS DATE ) ), INTERVAL 1 DAY)) AS $effectiveDateColNameInStgSrc
                                             |FROM $stgSourceWithEffectiveDateViewName
                                             |WHERE $effectiveDateColNameInStgSrc IS NOT NULL
                                             |""".stripMargin
      case EffectiveDateRule.WEEKDAYS => s"""
                                            |SELECT
                                            |    $effectiveDateColNameInStgSrc
                                            |FROM (
                                            |    SELECT EXPLODE(SEQUENCE(MIN( CAST( $effectiveDateColNameInStgSrc AS DATE ) ), MAX( CAST( $effectiveDateColNameInStgSrc AS DATE ) ), INTERVAL 1 DAY)) AS $effectiveDateColNameInStgSrc
                                            |    FROM $stgSourceWithEffectiveDateViewName
                                            |    WHERE $effectiveDateColNameInStgSrc IS NOT NULL
                                            |)
                                            |WHERE DAYOFWEEK(CAST( $effectiveDateColNameInStgSrc AS DATE ) ) NOT IN (1, 7) -- Excludes Sunday (1) and Saturday (7)
                                            |""".stripMargin
      case _ => throw new RuntimeException(s"""Etl ERROR: Effective Date Rule ${effectiveDateRule.toString} currently is not supported""")
    }

    val dfDateToProcess = spark.sql(sqlDatesToProcess)
      .where(s"$effectiveDateColNameInStgSrc IS NOT NULL")

    if (configDwEtl.getIsDebugDwLib) {
      dfDateToProcess.show(15);
      dfDateToProcess.printSchema()
    }
    dfDateToProcess
  }

  /**
   *
   * @return DataFrame with a single column "EffDate" of DateType. "EffDate" is a well known name to be used in the code
   */
  private def getEffectiveDatesToProcessForIncrLoadBasedOnConfig(configDwEtl: ConfigDwEtl, modelObjectName: String): DataFrame = {
    getEffectiveDatesToProcessForIncrLoad(configDwEtl, modelObjectName, dfDatesOverriddenBySpecificObjectAsOption = None)
  }

  private def getEffectiveDatesToProcessForIncrLoadBasedOnOverride(configDwEtl: ConfigDwEtl, modelObjectName: String, dfDatesOverriddenBySpecificObject: DataFrame): DataFrame = {
    getEffectiveDatesToProcessForIncrLoad(configDwEtl, modelObjectName, Some(dfDatesOverriddenBySpecificObject))
  }

  /**
   *
   * dfDatesOverriddenBySpecificObjectAsOption - df with one column ModelObject.effDateColumnNameInDatesToProcess
   *
   * @return DataFrame with a single column "EffDate" of DateType. "EffDate" is a well known name to be used in the code
   */
  private def getEffectiveDatesToProcessForIncrLoad(configDwEtl: ConfigDwEtl, modelObjectName: String, dfDatesOverriddenBySpecificObjectAsOption: Option[DataFrame]): DataFrame = {

    // Only process the first source with effective date
    val dfEffDate = if (configDwEtl.getStgSourceIdsForDeterminingEffectiveDates(modelObjectName).isEmpty && dfDatesOverriddenBySpecificObjectAsOption.isEmpty) {
      craeteEmptyEffeciveDatesDataFrame()
    }
    else {
      val (viewEffectiveDates, effectiveDateColName) = if (dfDatesOverriddenBySpecificObjectAsOption.isDefined) {
        val viewName = "DatesOverriddenBySpecificObject" + java.util.UUID.randomUUID.toString.replace("-", "_")
        dfDatesOverriddenBySpecificObjectAsOption.get.createOrReplaceTempView(viewName)
        (viewName, ModelObject.effDateColumnNameInDatesToProcess)
      }
      else {
        val (stgSourceIdForEffDate, (effectiveDateColNameInStgSrc, _, _)) = configDwEtl.getStgSourceIdsForDeterminingEffectiveDates(modelObjectName).head
        (stgSourceIdForEffDate, effectiveDateColNameInStgSrc)
      }

      val spark = SparkSession.builder().getOrCreate() // this gets previously created session
      val dfLastProcessedEffectiveDateTimestamp = ModelObject.getLastProcessedEffectiveDateTimestamp(configDwEtl, modelObjectName)

      // This result set can have null for source Id if some dimension does not use the source directly, like DimDate or static dimension.
      // This is fine because other dimensions will have the source that is used to determine dates, or even if we load just one dimension that does not have
      // a source it will still have effective date that is derived from the source that has "useToDetermineLoadEffectiveDates" indirectly,
      // like can be in case of DimDate
      // For example,
      // +-------------+--------------------+--------------------+
      // |SourceId     |LastProcessedEffDate|  MaxSourceTimestamp|
      // +-------------+--------------------+--------------------+
      // |        Teams|          2017-12-31|                null|
      // |         null|          2017-12-31|                null|
      // |   PlayByPlay|          2017-12-31|2021-03-31 12:16:...|
      // +-------------+--------------------+--------------------+
      val viewLastProcessedEffectiveDateTimestamp = "LastProcessedEffectiveDateTimestamp" + java.util.UUID.randomUUID.toString.replace("-", "_")
      dfLastProcessedEffectiveDateTimestamp.createOrReplaceTempView(viewLastProcessedEffectiveDateTimestamp)


      // Create a dataframe with current max timestamp for each source being processed.
      // This dataframe can be empty, in case there are no sources being processed, which is the cae for some dimensions
      // or the timestamp can be NULL, if datasource does not have it

      val sourceMinChangedEffDateMaxTimestampRecordSchema = new StructType(Array(
        new StructField("SourceId", StringType, true),
        new StructField("MinChangedEffDate", DateType, true),
        new StructField("MaxSourceTimestamp", TimestampType, true)
      ))
      val stgSourceIdsForModelObject = configDwEtl.getStgSourceIdsOfModelObject(modelObjectName)
      val sourcesMinChangedEffDateMaxTimestampList = for (
        stgSourceId <- stgSourceIdsForModelObject;
        ( minChangedEffDateAsOption, maxTimestampAsOption ) = ModelObject.getStagingSourceMinChangedEffDateAndMaxTimestamp(stgSourceId, configDwEtl, Some( dfLastProcessedEffectiveDateTimestamp ) );
        row = Row(stgSourceId, minChangedEffDateAsOption.getOrElse(null), maxTimestampAsOption.getOrElse(null))
      ) yield row

      // Sources can be empty and we still need this logic because effective dsates could be overridden

      // dfSourcesMaxTimestamp is a dataframe with all sources for a given model object and corresponding
      // max timestamps from the latest version of the source.
      // If the source does not have a timestamp  - it will be null.
      val dfSourcesMinChangedEffDateMaxTimestamp = if ( sourcesMinChangedEffDateMaxTimestampList.isEmpty ) {
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], sourceMinChangedEffDateMaxTimestampRecordSchema)
      }
      else {
        val sourcesMinChangedEffDateMaxTimestampRDD = spark.sparkContext.parallelize(sourcesMinChangedEffDateMaxTimestampList)
        spark.createDataFrame(sourcesMinChangedEffDateMaxTimestampRDD, sourceMinChangedEffDateMaxTimestampRecordSchema)
      }
      val viewSourcesMinChangedEffDateDateMaxTimestamp = "SourcesMinChangedEffDateDateMaxTimestamp" + java.util.UUID.randomUUID.toString.replace("-", "_")
      dfSourcesMinChangedEffDateMaxTimestamp.createOrReplaceTempView(viewSourcesMinChangedEffDateDateMaxTimestamp)

      if (configDwEtl.getIsDebugDwLib) {
        dfSourcesMinChangedEffDateMaxTimestamp.show(10);
        dfSourcesMinChangedEffDateMaxTimestamp.printSchema()
      }

      // view viewSourcesMinChangedEffDateDateMaxTimestamp can have no rows if the model object has no sources
      val sqlEffDateFromStgSource =
        s"""|
            |SELECT CAST( effDateSource.$effectiveDateColName AS DATE ) AS $effectiveDateColName
            |FROM $viewEffectiveDates AS effDateSource
            |WHERE CAST( effDateSource.$effectiveDateColName AS DATE ) >
            | LEAST(
            |       -- The first value in LEAST is last processed effective date for a given model object
            |       -- The second one is the earliest changed effective date on the source that can happen when
            |       -- old effective dates are reloaded during a day
            |       ( SELECT MAX( LastProcessedEffDate ) AS LastProcessedEffDate   -- this is the same select as next SELECT MAX( LastProcessedEffDate ) AS LastProcessedEffDate. Did it to get rid of CTEs which Spark does not like
            |         FROM $viewLastProcessedEffectiveDateTimestamp
            |         WHERE SourceId IN ( ${stgSourceIdsForModelObject.mkString("'", "', '", "'")} )
            |       ),
            |       IFNULL( (
            |         -- this will capture effective date changed for previously loaded
            |         -- effective dates if the the source was reloaded for some effective dates
            |           SELECT MIN( allSources.MinChangedEffDate )
            |           FROM $viewSourcesMinChangedEffDateDateMaxTimestamp AS allSources -- this view can be empty if model object does not have a source
            |               ), CAST( '2099-01-01' AS DATE ) )
            |      ) -- LEAST
            | -- below is the final part to define today's date
            | -- for the case when main source did not change since last load
            | -- but one of the sources changed (based on timestamp)
            |UNION
            |SELECT CAST( today_dt.today_date AS DATE ) AS $effectiveDateColName
            |FROM (
            |       SELECT current_date() AS today_date
            |   ) AS today_dt
            |WHERE
            |      NOT EXISTS ( SELECT 1 FROM $viewEffectiveDates ) -- i.e., the main source did not change and the list of dates is empty
            |  AND EXISTS  -- this part is the same as in previous union, see if any timestamp is greater than the previous loaded
            |  ( SELECT 1
            |    FROM $viewSourcesMinChangedEffDateDateMaxTimestamp AS allSources -- this view can be empty if model object does not have a source
            |      INNER JOIN $viewLastProcessedEffectiveDateTimestamp AS lastProcessed ON allSources.SourceId = lastProcessed.SourceId
            |    WHERE allSources.MaxSourceTimestamp IS NOT NULL
            |        AND lastProcessed.MaxSourceTimestamp IS NOT NULL
            |        AND allSources.MaxSourceTimestamp > lastProcessed.MaxSourceTimestamp
            |  )
            |""".stripMargin


      val dfEffDateFromStgSourceFromSql = spark.sql(sqlEffDateFromStgSource)
      if (configDwEtl.getIsDebugDwLib) {
        dfEffDateFromStgSourceFromSql.show(5);
        dfEffDateFromStgSourceFromSql.printSchema()
      }

      val dfEffDateFromStgSource = if (dfDatesOverriddenBySpecificObjectAsOption.isDefined) {
        dfEffDateFromStgSourceFromSql
      }
      else {
        val (_, (_, effectiveDateRule, _)) = configDwEtl.getStgSourceIdsForDeterminingEffectiveDates(modelObjectName).head
        getEffectiveDatesBasedOnTheRule(effectiveDateRule, dfEffDateFromStgSourceFromSql, effectiveDateColName)
      }

      // Normalize the dates df by dropping specific effective date column from the source and replacing it with "well known" name "EffDate"
      if ( effectiveDateColName != ModelObject.effDateColumnNameInDatesToProcess )
        dfEffDateFromStgSource.withColumn(ModelObject.effDateColumnNameInDatesToProcess, dfEffDateFromStgSource(effectiveDateColName).cast(DateType))
        .drop(effectiveDateColName)
      else
        dfEffDateFromStgSource
    }

    if (configDwEtl.getIsDebugDwLib) {
      dfEffDate.show(5);
      dfEffDate.printSchema()
    }

    dfEffDate
  }


  /**
   * The result has a one row with latest effective date for each datasource for a given dimensional model object
   *
   * @param configDwEtl
   * @param modelObjectName
   * @return
   */
  private def getLastProcessedEffectiveDateTimestamp(configDwEtl: ConfigDwEtl, modelObjectName: String): DataFrame = {
    val spark = SparkSession.builder().getOrCreate() // this gets previously created session

    // if below require changed, modify the code that uses getEtlLogFilePath
    require(configDwEtl.isEtlLogParquetFile == true, """DW ETL ERROR: DW Etl currently only supports parquet file for Etl log """)

    val dfEtlLog = spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .load(configDwEtl.getEtlLogFilePath)
    dfEtlLog.createOrReplaceTempView("EtlLog")

    // Select min effective date and min timestamp (if available) from the last load of
    // all dimensional model objects being loaded
    val stgSourceIdsForModelObject = configDwEtl.getStgSourceIdsOfModelObject(modelObjectName)

    val dfLastProcessedEffectiveDateTimestamp = if ( stgSourceIdsForModelObject.nonEmpty ) {
      val sqlLastProcessedEffectiveDateTimestamp
      =
        s"""|
            |SELECT SourceId, LastProcessedEffDate, MaxSourceTimestamp
            |FROM (
            |  SELECT ROW_NUMBER() OVER ( PARTITION BY SourceId ORDER BY ProcessedOn DESC ) AS ReverseLoadOrder,
            |         *
            |  FROM EtlLog
            |  WHERE LastProcessedEffDate IS NOT NULL -- skip loads with no data to load. The will have NULL LastProcessedEffDate. All other loads must have non-NULL
            |     AND ModelObjectName = '$modelObjectName'
            |     AND SourceId IN ( ${stgSourceIdsForModelObject.mkString("'", "', '", "'")} )
            |   ${if (configDwEtl.getRerunEtlAfter.isDefined) s" AND ProcessedOn <= CAST( '${configDwEtl.getRerunEtlAfter.get}' AS TIMESTAMP ) " else ""}
            |) AS LatestLoad
            |WHERE ReverseLoadOrder = 1
					  |  """.stripMargin
      spark.sql(sqlLastProcessedEffectiveDateTimestamp)
    }
    else {
      val sourceEffDateTimestampRecordSchema = new StructType(Array(
        new StructField("SourceId", StringType, true),
        new StructField("LastProcessedEffDate", DateType, true ),
        new StructField("MaxSourceTimestamp", TimestampType, true)
      ))
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], sourceEffDateTimestampRecordSchema)
    }

    if (configDwEtl.getIsDebugDwLib) {
      dfLastProcessedEffectiveDateTimestamp.show(5);
      dfLastProcessedEffectiveDateTimestamp.printSchema()
    }

    dfLastProcessedEffectiveDateTimestamp
  }

  private def getLastProcessedTimestamp(configDwEtl: ConfigDwEtl, modelObjectName: String): Map[String, Option[SqlTimestamp]] = {
    val dfLastProcessedEffectiveDateTimestamp = ModelObject.getLastProcessedEffectiveDateTimestamp(configDwEtl, modelObjectName)

    if (configDwEtl.getIsDebugDwLib) {
      dfLastProcessedEffectiveDateTimestamp.show(20);
      dfLastProcessedEffectiveDateTimestamp.printSchema()
    }

    dfLastProcessedEffectiveDateTimestamp
      .select(col("SourceId"), col("MaxSourceTimestamp"))
      .collect()
      .filter {
        case Row(null, null) => false // for some dimensions, that do not have source specified in the configuration, the SourceId can be null, like for DimDate
        case _ => true
      }
      .map {
        case Row(sourceId: String, sourceTimestamp: SqlTimestamp) => sourceId -> Some(sourceTimestamp)
        case Row(sourceId: String, null) => sourceId -> None
      }
      .toMap
  }

  private def createEtlRecordDataFrame(
      modelObjectName: String,
      sourceId: Option[String],
      maxTimestampAsOption: Option[SqlTimestamp],
      lastProcessedEffDateAsOption: Option[Date],
      isInitialLoad: Boolean,
      isRerun: Boolean,
      durationSec: Double): DataFrame = {

    val spark = SparkSession.builder().getOrCreate() // this gets previously created session

    val etlRecordSchema = new StructType(Array(
      new StructField("JobId", StringType, true),
      new StructField("ModelObjectName", StringType, true),
      new StructField("SourceId", StringType, true),
      new StructField("LastProcessedEffDate", DateType, true),
      new StructField("MaxSourceTimestamp", TimestampType, true),
      new StructField("IsInitialLoad", BooleanType, true),
      new StructField("IsRerun", BooleanType, true),
      new StructField("LoadDurationSec", DoubleType, true)
    ))

    val etlRecord = Seq(
      Row(
        jobId,
        modelObjectName,
        sourceId.getOrElse(null),
        lastProcessedEffDateAsOption.getOrElse(null),
        maxTimestampAsOption.getOrElse(null),
        isInitialLoad,
        isRerun,
        durationSec
      ))

    val etlRecordRDD = spark.sparkContext.parallelize(etlRecord)
    val dfEtlRecord = spark.createDataFrame(etlRecordRDD, etlRecordSchema)
      .withColumn("ProcessedOn", current_timestamp())
      .withColumn("ProcessedBy", lit(System.getProperty("user.name"))) // identity of who runs the job
      .setNullableStateForAllColumns(true)
    dfEtlRecord
  }

  /**
   * The created records will be used for incremental load
   *
   */
  private[etl] def createEtlLogRecord(
      modelObjectName: String, configDwEtl: ConfigDwEtl, stgSourceIds: List[String], datesToProcess: List[Date],
      isInitialLoad: Boolean, isRerun: Boolean, durationSec: Double): Unit = {

    // loop via every source and record the last timestamp used and the last effective date used for this dimension

    val dfEtlRecordList = for (
      stgSourceId <- stgSourceIds;
      ( _, maxTimestampAsOption ) = ModelObject.getStagingSourceMinChangedEffDateAndMaxTimestamp(stgSourceId, configDwEtl, None);
      dfEtlRecord = ModelObject.createEtlRecordDataFrame(modelObjectName, Some(stgSourceId), maxTimestampAsOption, datesToProcess.lastOption, isInitialLoad, isRerun, durationSec);
      test = if (configDwEtl.getIsDebugDwLib) {
        dfEtlRecord.show(4)
        dfEtlRecord.printSchema()
      }
    ) yield dfEtlRecord // maxTimestamp would be None if the source does not have it

    // Some dimensions do not have any sources so it is possible not to have any records
    // In taht case create a record with effective date only
    val dfEtlRecordsForAllSources = if (!dfEtlRecordList.isEmpty) {
      dfEtlRecordList.reduceLeft((df1, df2) => df1.union(df2))
    } else {
      ModelObject.createEtlRecordDataFrame(modelObjectName, None, None, datesToProcess.lastOption, isInitialLoad, isRerun, durationSec);
    }

    if (configDwEtl.getIsDebugDwLib) {
      dfEtlRecordsForAllSources.show(4)
      dfEtlRecordsForAllSources.printSchema()
    }

    // if below require changed, modify the code that uses getEtlLogFilePath
    require(configDwEtl.isEtlLogParquetFile == true, """DW ETL ERROR: DW Etl currently only supports parquet file for Etl log """)

    FileHelper.saveDataFrameAsParquetAndMoveToParentDir(dfEtlRecordsForAllSources, "ModelEtlLog", configDwEtl.getEtlLogFilePath)
  }

}

private[etl] abstract class ModelObject(private val modelObjectName: String) {

  private[etl] val configDwEtl: ConfigDwEtl = ModelObject.configDwEtl
  private[etl] val isDebugDwLib = configDwEtl.getIsDebugDwLib

  private[etl] val sparkModelObject: SparkSession = SparkSession.builder().getOrCreate() // this gets previously created session

  import sparkModelObject.implicits._

  // lastProcessedStgSourceTimestamp will be empty on initial load
  // It is used to load data after the timestamp that was last loaded
  private val lastProcessedStgSourceTimestamp: Map[String, Option[SqlTimestamp]] = if (!isInitialLoad) ModelObject.getLastProcessedTimestamp(configDwEtl, modelObjectName) else Map.empty[String, Option[SqlTimestamp]]
  private var isStgSourceChangedSinceLastLoadIfKnown: Map[String, Option[Boolean]] = Map.empty
  private val viewWithDatesToProcess: String = "DatesToProcess_" + modelObjectName + "_" + java.util.UUID.randomUUID.toString.replace("-", "_")

  protected def effDateColumnNameInDatesToProcess: String = ModelObject.effDateColumnNameInDatesToProcess

  protected def isInitialLoad: Boolean = configDwEtl.getIsInitialLoad

  protected def datesToProcessView: String = viewWithDatesToProcess

  protected def isStgSourceChangedSinceLastLoad(stgSrcView: String): Boolean = isStgSourceChangedSinceLastLoadIfKnown
    .getOrElse(stgSrcView, Some(true)) // When a name passed to this function is not the correct source, just return true
    .getOrElse(true)

  protected def getLastProcessedStgSourceTimestamp(stgSrcView: String): Option[SqlTimestamp] = lastProcessedStgSourceTimestamp
    .getOrElse(stgSrcView, None) // When a name passed to this function is not the correct source, return None

  protected def preProcess(stgSrcViewsWithIdNames: List[String], datesToProcess: List[Date]): Unit = () //preProcess is called before the etl method starts processing. override to create multiple views on the same source

  protected def postProcess(stgSrcViewsWithIdNames: List[String], datesToProcess: List[Date], dfModelObject: DataFrame): DataFrame = dfModelObject // postProcess is called right before the df is saved. Overrode if needed

  protected def getCustomDatesToProcess(dfDatesToProcessBasedOnConfig: DataFrame): Option[DataFrame] = None // Override this method to create custom list of dates to process

  private[etl] def etlModelObject(stgSrcViewsWithIdNames: List[String], datesToProcess: List[Date]): Option[DataFrame] // Both dimensions and facts must override this method

  private[etl] def checkLoadResultForDuplicateColumns(dfSrc: DataFrame, stgSrcView: String): Unit = {
    val srcSchema: StructType = dfSrc.schema
    // check for duplicate names in the result set
    val duplicates = srcSchema.names.diff(srcSchema.names.distinct).distinct
    if (!duplicates.isEmpty) {
      val errorMessage = s"""Etl Runner ERROR: Data for $modelObjectName $getTypeOfCurrentInstance ${if (stgSrcView == ModelObject.viewNameForNonExistentDataSource) "" else "from \"" + stgSrcView + "\" source "}has duplicate column names: ${duplicates.mkString(", ")} """
      throw new RuntimeException(errorMessage)
    }
  }

  private[etl] def checkForMismatchedFieldNamesInActualAndExpectedSchemas(dfSrc: DataFrame, stgSrcView: String, expectedSchema: StructType, expectedSchemaHasDimensionKeyColumn: Boolean = false): Unit = {
    val srcSchema: StructType = dfSrc.schema
    val errorMessage = createErrorMessageForMismatchedFieldsInActualAndExpectedSchemas(stgSrcView, srcSchema, expectedSchema, expectedSchemaHasDimensionKeyColumn)
    if (!errorMessage.isEmpty)
      throw new RuntimeException(errorMessage)
    require(srcSchema.names.toSet == expectedSchema.names.toSet) // confirm that names match. They still may be in different order
  }

  private[etl] def checkFieldsWithSameNamesForTypeCompatibility(dfSrc: DataFrame, expectedSchema: StructType, stgSrcView: String): Unit = {
    val srcSchema: StructType = dfSrc.schema

    val errorMessages = for (
      srcField <- srcSchema;

      // find corresponding field in expected schema
      expectedFieldAsOption: Option[StructField] = expectedSchema.find(_.name == srcField.name);
      errorMessage = if (expectedFieldAsOption.isDefined) {
        val expectedField = expectedFieldAsOption.get;
        val typesCompatible = (srcField.dataType, expectedField.dataType) match {
          // 1. Primitive Type Exact Match
          case (src, tgt) if src == tgt => true

          // 2. Numeric Upcasting (I want to allow safe numeric widening)
          case (ByteType, ShortType | IntegerType | LongType) => true
          case (ShortType, IntegerType | LongType) => true
          case (IntegerType, LongType) => true
          case (FloatType, DoubleType) => true
          case (DateType, TimestampType) => true

          // Incompatible types
          case _ => false
        };

        // errorMessage = if( !Cast.canCast(srcField.dataType, toField.dataType) ) {
        val errorMessageForMatchedField = if (!typesCompatible) {
          s"""\n data type \"${srcField.dataType.typeName}\" of field \"${srcField.name}\" is not compatible with expected data type \"${expectedField.dataType.typeName}\".  Correct the type of the field in the code (e.g., via CAST) or in the configuration """
        }
        else
          ""

        errorMessageForMatchedField
      }
      else
        ""
    ) yield (errorMessage)

    val uniqueErrorMessages = errorMessages.toSet - ""

    if (!uniqueErrorMessages.isEmpty) {

      val finalErrorMessage =
        s"""Etl Runner ERROR: Data for $modelObjectName $getTypeOfCurrentInstance ${if (stgSrcView == ModelObject.viewNameForNonExistentDataSource) "" else "from \"" + stgSrcView + "\" source"} has errors with data types: """ +
          uniqueErrorMessages.mkString(", ")
      throw new RuntimeException(finalErrorMessage)
    }
  }

  private def createErrorMessageForMismatchedFieldsInActualAndExpectedSchemas(stgSrcView: String, actualSchema: StructType, expectedSchema: StructType, expectedSchemaHasDimensionKeyColumn: Boolean): String = {
    val baseErrorMessage = if (expectedSchema.length == actualSchema.length)
      s"""Etl Runner ERROR: The following discrepancies in column names for \"$modelObjectName\" $getTypeOfCurrentInstance may be the reason for the error: """
    else
      s"""Etl Runner ERROR: Data for \"$modelObjectName\" $getTypeOfCurrentInstance ${if (stgSrcView == ModelObject.viewNameForNonExistentDataSource) "" else "from \"" + stgSrcView + "\" source "}has incorrect number of columns - expected ${expectedSchema.length} ${if (!expectedSchemaHasDimensionKeyColumn) "(that excludes surrogate key which must not be included in the load result) " else ""}vs. actual ${actualSchema.length} """

    // if there is at least one field with the same name, assume the intention is to have matching names and in that case determine fields missing from either schema
    val commonFields = actualSchema.names.intersect(expectedSchema.names)
    val errorMessage: String = if (commonFields.length > 0) {
      val fieldsMissingFromLoadedDimension = expectedSchema.names.diff(actualSchema.names).mkString(", ")
      val fieldsMissingFromConfiguration = actualSchema.names.diff(expectedSchema.names).mkString(", ")
      (if (!fieldsMissingFromLoadedDimension.isEmpty)
        s"""\nThe following fields defined in the configuration are missing from the $getTypeOfCurrentInstance data: $fieldsMissingFromLoadedDimension """
      else
        "") +
        (if (!fieldsMissingFromConfiguration.isEmpty)
          s"""\nThe following fields defined in the $getTypeOfCurrentInstance data do not exist in the configuration${if (!expectedSchemaHasDimensionKeyColumn && isCurrentInstanceDimension) " (or is dimension key that should not be included) " else ""}: $fieldsMissingFromConfiguration """
        else
          "")
    }
    else
      s"""\nNone of the fields' names in the result match field names in \"$modelObjectName\" $getTypeOfCurrentInstance configuration. Use field alias to associate columns in the result with columns in the \"$modelObjectName\" $getTypeOfCurrentInstance configuration."""

    if (expectedSchema.length == actualSchema.length && errorMessage.isEmpty)
      ""
    else
      baseErrorMessage + errorMessage
  }

  private def isCurrentInstanceDimension: Boolean = {
    this match {
      case d: Dim => true
      case f: Fact => false
      case _ => throw new RuntimeException("Unknown ModelObject type")
    }
  }

  private def getTypeOfCurrentInstance: String = {
    this match {
      case d: Dim => "dimension"
      case f: Fact => "fact table"
      case _ => throw new RuntimeException("Unknown ModelObject type")
    }
  }

  private[etl] def getName: String = modelObjectName

  private[etl] def getEffectiveDatesToProcess(): List[Date] = {
    val dfEffDates = ModelObject.getEffectiveDatesToProcessBasedOnConfig(configDwEtl, modelObjectName)

    val datesToProcess: List[Date] = {
      val dfDatesToProcessAsOption = getCustomDatesToProcess(dfEffDates) // Call override for dates. The override may or may not be defined
      val dfFinalDatesToProcess = if (dfDatesToProcessAsOption.isDefined) { // i.e., the dates got overridden
        val dfSpecificDates = dfDatesToProcessAsOption
          .get
          .withColumnRenamed(dfDatesToProcessAsOption.get.schema.fieldNames(0), effDateColumnNameInDatesToProcess)

        if (isDebugDwLib) {
          dfSpecificDates.show(4)
          dfSpecificDates.printSchema()
        }

        if (isInitialLoad) {
          dfSpecificDates
        }
        else {
          ModelObject.getEffectiveDatesToProcessForIncrLoadBasedOnOverride(configDwEtl, modelObjectName, dfSpecificDates)
        }
      }
      else {
        dfEffDates
      }

      dfFinalDatesToProcess.createOrReplaceTempView(viewWithDatesToProcess)

      dfFinalDatesToProcess
        .select(effDateColumnNameInDatesToProcess)
        .map(_.getDate(0))
        .collect()
        .toList
        .sortWith(_.compareTo(_) <= 0)
    }

    datesToProcess
  }

  private[etl] def loadStagingSources(): List[String] = {

    val list = for (
      stgSourceId <- configDwEtl.getStgSourceIdsOfModelObject(modelObjectName);
      dfStgSource = ModelObject.getStagingSource(stgSourceId)
    ) yield stgSourceId -> dfStgSource

    for ((stgSourceId, dfStgSource) <- list) {
      dfStgSource.createOrReplaceTempView(stgSourceId) // need this as view to determine new versions
    }

    for (stgSourceId <- configDwEtl.getStgSourceIdsOfModelObject(modelObjectName)) {
      val ( _, timestampMaxCurrentAsOption ) = ModelObject.getStagingSourceMinChangedEffDateAndMaxTimestamp(stgSourceId, configDwEtl, None )
      isStgSourceChangedSinceLastLoadIfKnown += (stgSourceId -> (if (timestampMaxCurrentAsOption.isDefined && lastProcessedStgSourceTimestamp.contains(stgSourceId)
        && lastProcessedStgSourceTimestamp(stgSourceId).isDefined)
        Some(timestampMaxCurrentAsOption.get.compareTo(lastProcessedStgSourceTimestamp(stgSourceId).get) > 0) else None))
    }

    // This returns a list of TempView names that are the same as stgSourceId
    list.map(_._1)
  }

}
