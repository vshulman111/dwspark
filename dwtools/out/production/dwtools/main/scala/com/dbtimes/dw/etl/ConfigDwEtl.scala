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

import java.sql.Timestamp
import java.nio.file.{FileSystems, Files, Path => JavaPath}
import java.text.SimpleDateFormat
import java.util.Calendar
import EffectiveDateRule.EffectiveDateRule
import FactProcessMode.FactProcessMode
import com.dbtimes.dw.common._
import com.typesafe.config.Config
import org.apache.spark.sql.types.{StructField, StructType}

import scala.jdk.CollectionConverters._
import scala.collection.immutable.ListMap
import scala.collection.mutable

final private[etl] class ConfigDwEtl(private[etl] val configDwEtl: Config) extends ConfigDwJobsCommon {

  private[etl] def validateModelConfiguration():  Seq[String] = {

    var errors: mutable.Seq[String] = mutable.Seq.empty[String]

    // Dimension checks
    // -----------------------------------------------------------------

    // Check: make sure dimension names match pattern
    {
      val pattern = configDwEtl.getString("dwEtl.dimAuthority.dimensionNamePattern")
      val dimNamePatternRegEx = pattern.r
      val errorDims = for {dimName <- getDimNamesToLoad // check only dimensions that have "isLoad" flag set to true
                           firstMatchStrAsOption = dimNamePatternRegEx.findFirstIn(dimName)
                           if ( !firstMatchStrAsOption.isDefined || firstMatchStrAsOption.getOrElse("") != dimName )
                           } yield dimName
      if (!errorDims.isEmpty) {
        errors = errors :+ s"""The following dimension name(s) - ${errorDims.mkString(", ")} - do not match the dimension name pattern $pattern """
      }
    }

    // Check: make sure all dimension names are unique
    val dimNamesToLoad: List[String] = getDimNamesToLoad
    val duplicateDimNames = dimNamesToLoad.groupBy(identity)
      .collect { case (x, ys) if ys.size > 1 => x }
    if (!duplicateDimNames.isEmpty) {
      errors = errors :+ s"""The following dimension names are duplicates: ${duplicateDimNames.mkString("'", "', '", "'")}. All dimension names must be unique."""
    }

    // Check: make sure all fact table names are unique
    val factNamesToLoad: List[String] = getFactNamesToLoad
    val duplicateFactNames = factNamesToLoad.groupBy(identity)
      .collect { case (x, ys) if ys.size > 1 => x }
    if (!duplicateFactNames.isEmpty) {
      errors = errors :+ s"""The following fact table names are duplicates: ${duplicateFactNames.mkString("'", "', '", "'")}. All fact table names must be unique."""
    }

    // Check: make sure each dimension has schema - implemented in schema validation
    // Check: each dimension must have dimension key - implemented in schema validation
    // Check: Each dimension must have at least one natural key column - implemented in schema validation
    // Check: If dimension key is a surrogate key, its type must be Long - implemented in schema validation
    // Check: Dimension key column must have unknown value - implemented in schema validation
    // Check: Each natural key column must have unknown value - implemented in schema validation

    val stgSources = configDwEtl.getConfigList("dwEtl.stgSources").asScala.toList
    // Check: make sure the sources have "moniker" attribute defined - implemented in schema validation
    // Check: make sure all sources are files, i.e., "fileSource" attribute defined - implemented in schema validation
    // Check: make sure the file sources have "fileSource.path" attribute defined - implemented in schema validation
    // Check: make sure the sources are parquet files - implemented in schema validation

    errors.toSeq
  }

  private[etl] def getDwClassName: Option[String] = {
    if (configDwEtl.hasPath("dwEtl.dwClassName")) Some(configDwEtl.getString("dwEtl.dwClassName")) else None
  }

  private[etl] def getDimAuthorityPackageName: String = configDwEtl.getString("dwEtl.dimAuthority.packageName")

  private[etl] def getDataMartPackageName: String = configDwEtl.getString("dwEtl.dataMart.packageName")

  private[etl] def getIsLoadDimensions: Boolean = {
    if ("LoadDims(Facts)?".r.findFirstMatchIn(configDwEtl.getString("dwEtl.jobType")).isDefined) true else false
  }

  private[etl] def getIsLoadFacts: Boolean = {
    if ("Load(Dims)?Facts".r.findFirstMatchIn(configDwEtl.getString("dwEtl.jobType")).isDefined) true else false
  }

  private def getDimensionConfiguration(dimName: String): Config = {
    val dimensions = configDwEtl.getConfigList("dwEtl.dimensions").asScala.toList
    val dimension = dimensions find {
      case dimension: Config => dimension.getString("name") == dimName
    }
    if (!dimension.isDefined)
      throw new RuntimeException("""ETL  ERROR: dimension """ + dimName + """ is not defined """)
    dimension.get
  }

  private[etl] def getDimNamesToLoad: List[String] = {
    val dimensions = configDwEtl.getConfigList("dwEtl.dimensions").asScala.toList
    val dimensionsToLoad = dimensions filter {
      case dimension: Config => {
        if ((dimension.hasPath("isLoad") && dimension.getBoolean("isLoad")) || !dimension.hasPath("isLoad")) // default is to load
          true
        else
          false
      }
    }
    dimensionsToLoad map { case dimension: Config => dimension.getString("name") } // return dimension name only
  }

  private[etl] def getIsDebugDwLib: Boolean = {
    if (configDwEtl.hasPath("dwEtl.isDebugDwLib")) {
      configDwEtl.getBoolean("dwEtl.isDebugDwLib")
    } else {
      false // default - no debugging
    }
  }

  private[etl] def getIsInitialLoad: Boolean = {
    configDwEtl.getBoolean("dwEtl.isInitialLoad")
  }

  private[etl] def isEtlLogParquetFile: Boolean = {
    if (configDwEtl.hasPath("dwEtl.fileEtlLog") && configDwEtl.hasPath("dwEtl.fileEtlLog.parquet")) {
      true
    } else {
      false
    }
  }

  private[etl] def getEtlLogFilePath: String = configDwEtl.getString("dwEtl.fileEtlLog.path")

  private[etl] def getRerunEtlAfter: Option[String] = {
    if (configDwEtl.hasPath("dwEtl.rerunEtlAfter")) {
      Some(configDwEtl.getString("dwEtl.rerunEtlAfter"))
    } else {
      None
    }
  }

  ////////////////////////////////////////////////////////////////////////////
  ////////  Facts
  ////////////////////////////////////////////////////////////////////////////

  private[etl] def getFactNamesToLoad: List[String] = {
    val facts = configDwEtl.getConfigList("dwEtl.facts").asScala.toList
    val factsToLoad = facts filter {
      case fact: Config =>
        if ((fact.hasPath("isLoad") && fact.getBoolean("isLoad")) || !fact.hasPath("isLoad")) // default is to load
          true
        else
          false
    }
    factsToLoad map { case fact: Config => fact.getString("name") } // return fact name only
  }


  private[etl] def getStgSourceMonikersOfFact(factName: String): List[String] = {
    val factConfig = getFactConfiguration(factName)
    if (!factConfig.hasPath("factStgSources")) {
      List.empty[String]
    }
    else {
      val factStgSources = factConfig.getConfigList("factStgSources").asScala.toList
      factStgSources map {
        case stgSourceMoniker: Config => stgSourceMoniker.getString("moniker")
      }
    }
  }

  private def getFactConfiguration(factName: String): Config = {
    val facts = configDwEtl.getConfigList("dwEtl.facts").asScala.toList
    val fact = facts find {
      case fact: Config => fact.getString("name") == factName
    }
    if (!fact.isDefined) {
      throw new RuntimeException("""ETL  ERROR: fact """ + factName + """is not defined """)
    }
    fact.get
  }

  private[etl] def getFactSchema(factName: String): StructType = super.getSchema(getFactConfiguration(factName), "schema")

  private[etl] def getSchemaForMergeKeys(factName: String): StructType = {
    val mergeKeys = getMergeKeysCols(factName)

    val factSchema = getFactSchema(factName)

    val mergeKeysColumns = factSchema filter {
      case StructField(c, t, _, m) => if (mergeKeys.contains(c)) true else false
    }
    StructType(mergeKeysColumns)
  }


  private[etl] def getDimensionsForSettingForeignKeysOrLoadingFact(factName: String): List[String] = {

    val dimensionsForSettingForeignKeys = super.getSchemaDistinctPropertyValuesWithFlag(
      getFactConfiguration(factName), "schema", "underlyingDim", "isForSettingForeignKey", true, Some(false))

    // Dimensions can be used as a source for loading fact table
    // Combine these dimensions with the ones needed to set fact table keys
    val dimSourceNamesOfFact = getDimSourceNamesOfFact(factName)

    val dimensionsForSettingForeignKeysOrLoadingFact = dimensionsForSettingForeignKeys
      .union(dimSourceNamesOfFact)
      .distinct
    dimensionsForSettingForeignKeysOrLoadingFact
  }

  private[etl] def getDimSourceNamesOfFact(factName: String): List[String] = {
    val factConfig = getFactConfiguration(factName)
    if (!factConfig.hasPath("factDimSources")) {
      List.empty[String]
    }
    else {
      val factDimSources = factConfig.getConfigList("factDimSources").asScala.toList
      factDimSources map {
        case dimSourceName: Config => dimSourceName.getString("name")
      }
    }
  }

  private[etl] def getEffectiveDateColNameForSettingTypeTwoKeys(factName: String): Option[String] = {
    val effDateCols = super.getSchemaColNamesWithFlag(getFactConfiguration(factName), "schema", "isEffDateForTypeTwo", true, Some(false))

    if (effDateCols.isEmpty) None else Some(effDateCols.head)
  }

  /**
   * Iterate over all fact tables to be loaded and select all columns used to set key for this dimension.
   *
   * @param dimName
   * @return
   */
  private[etl] def getDimensionColsToSetKeyForAllFacts(dimName: String): List[String] = {

    val listOfColsFromThisDimensionToSetKeyForEachFact = for (
      factName <- getFactNamesToLoad;
      allDimensionsColsForSettingForeignKeys = super.getSchemaDistinctPropertyValuesWithOtherPropertyAndFlag(
        getFactConfiguration(factName), "schema", "underlyingDimCol",
        "underlyingDim", dimName, "isForSettingForeignKey", true, Some(false));
      dimensionCols = getDimCols(dimName);
      thisDimensionColsForSettingForeignKeys = allDimensionsColsForSettingForeignKeys filter {
        case dimensionColumn => dimensionCols.contains(dimensionColumn)
      }
    ) yield {
      thisDimensionColsForSettingForeignKeys
    }

    // combine all columns into one list. Because each fact table is likely to use the same columns
    // the columns in this list will be duplicated as many times as there are fact tables that use given dimension
    val listOfColsFromThisDimensionToSetKey = listOfColsFromThisDimensionToSetKeyForEachFact.reduceLeft((listOne, listTwo) => listOne ++ listTwo)

    listOfColsFromThisDimensionToSetKey.distinct
  }

  /**
   * Iterate over all fact tables to be loaded and select all columns from this dimension used to load fact table
   *
   * @param dimName
   * @return - the list of all columns that are used to load fact table.
   *         This list can be empty if this dimension is not used for loading any fact table
   */
  private[etl] def getDimensionColsUsedInLoadingAllFacts(dimName: String): List[String] = {
    // For each fact table determine if this dimension is used to load a fact table
    // and, if yes, which dimension columns are used. then create a union of all columns
    val listOfColsFromThisDimensionToLoadEachFact = for (
      factName <- getFactNamesToLoad;
      factConfig = getFactConfiguration(factName);
      dimensionSources = getDimSourceNamesOfFact(factName);

      // get columns from only one this dimension from all dimension sources for this fact table
      allDimensionColumnsUsedForLoadingFact = if (dimensionSources.exists(_ == dimName)) {
        val dimensionSourcesToLoadFact = factConfig.getConfigList("factDimSources").asScala.toList
        val dimensionSourceToLoadFact = dimensionSourcesToLoadFact find {
          case dim: Config => dim.getString("name") == dimName
        }

        // if dimension is in the list it may have a list of columns used in the load of a fact table
        // or the list of columns can be absent in which case all columns will be returned
        if ( dimensionSourceToLoadFact.get.hasPath("schema")) {
          super.getSchemaColNames(getDimensionConfiguration(dimName), "schema")
        }
        else // get all columns
        {
          getDimCols(dimName)
        }
      }
      else {
        List.empty[String]
      };
      dimensionCols = getDimCols(dimName);

      // allDimensionColumnsUsedForLoadingFact can be empty if this dimension is not a source for the fact
      thisDimensionColsForLoadingFact = allDimensionColumnsUsedForLoadingFact filter {
        case dimensionColumn => dimensionCols.contains(dimensionColumn)
      }
    ) yield {
      thisDimensionColsForLoadingFact
    }

    // combine all columns into one list. Because each fact table is likely to use the same columns
    // the columns in this list will be duplicated as many times as there are fact tables that use given dimension
    val listOfColsFromThisDimensionToLoadFact = listOfColsFromThisDimensionToLoadEachFact.reduceLeft((listOne, listTwo) => listOne ++ listTwo)

    listOfColsFromThisDimensionToLoadFact.distinct
  }

  private[etl] def getPartitionByCols(factName: String): List[String] = {
    super.getSchemaColNamesWithFlag(getFactConfiguration(factName), "schema", "isForPartition", true, Some(false))
  }

  private[etl] def getMergeKeysCols(factName: String): List[String] = {
    super.getSchemaColNamesWithFlag(getFactConfiguration(factName), "schema", "isMergeKey", true, Some(false))
  }

  private[etl] def getForeignKeys(factName: String): List[String] = {
    super.getSchemaDistinctPropertyValues(getFactConfiguration(factName), "schema", "foreignKey");
  }

  private[etl] def getFactMeasures(factName: String): List[String] = {
    super.getSchemaColNamesWithFlag(getFactConfiguration(factName), "schema", "isMeasure", true, Some(false))
  }

  private[etl] def getDimensionForSettingForeignKey(factName: String, key: String): String = {
    val dimNameList = super.getSchemaDistinctPropertyValuesWithOtherPropertyAndFlag(
      getFactConfiguration(factName), "schema", "underlyingDim",
      "foreignKey", key, "isForSettingForeignKey", true, Some(false));

    // All names in the list should be the same
    dimNameList.head
  }

  /**
   * This method returns a map of columns from underlying dimension with corresponding columns from fact schema.
   * For example to set a PlayTypeKey there for fields needed from DimPlayType. In this schema the names of the
   * fields in the dimension happen to be the same as in fact schema, but the can be different
   * { "colName" : "Formation", "colType" : "String", "isForSettingForeignKey" : "true", "foreignKey" : "PlayTypeKey", "underlyingDim" : "DimPlayType", "underlyingDimCol" : "Formation" },
   * { "colName" : "PlayType", "colType" : "String", "isForSettingForeignKey" : "true", "foreignKey" : "PlayTypeKey", "underlyingDim" : "DimPlayType", "underlyingDimCol" : "PlayType" },
   * { "colName" : "PassType", "colType" : "String", "isForSettingForeignKey" : "true", "foreignKey" : "PlayTypeKey", "underlyingDim" : "DimPlayType", "underlyingDimCol" : "PassType" },
   * { "colName" : "PenaltyType", "colType" : "String", "isForSettingForeignKey" : "true", "foreignKey" : "PlayTypeKey", "underlyingDim" : "DimPlayType", "underlyingDimCol" : "PenaltyType" },
   *
   *  The return value in this case will be map like this
   *  { "Formation"   , "Formation"   },
   *  { "PlayType"    , "PlayType"    },
   *  { "PassType"    , "PassType"    },
   *  { "PenaltyType" , "PenaltyType" }
   *
   * @param factName
   * @param key - the foreing key name, e.g., "PlayTypeKey"
   * @return - a map of dimension columns with corresponding fact table columns needed to set this key
   */
  private[etl] def getMapDimColsToFactCols(factName: String, key: String): Map[String, String] = {
    val dimCols = super.getSchemaDistinctPropertyValuesWithOtherPropertyAndFlag(
      getFactConfiguration(factName), "schema", "underlyingDimCol",
      "foreignKey", key, "isForSettingForeignKey", true, Some(false));
    val factCols = super.getSchemaDistinctPropertyValuesWithOtherPropertyAndFlag(
      getFactConfiguration(factName), "schema", "colName",
      "foreignKey", key, "isForSettingForeignKey", true, Some(false));

    val dimAndFactCols = dimCols zip factCols

    dimAndFactCols.toMap
  }

  private[etl] def getColsForSettingForeignKeys(factName: String): List[String] = {
    super.getSchemaColNamesWithFlag(
      getFactConfiguration(factName), "schema", "isForSettingForeignKey", true, Some(false))
  }

  private[etl] def getIsFactsDestinationFile: Boolean = if (configDwEtl.hasPath("dwEtl.dataMart.fileFactsDestination.parquet")) true else false

  private[etl] def getIsFactsDestinationFileParquet: Boolean = if (getIsFactsDestinationFile && configDwEtl.hasPath("dwEtl.dataMart.fileFactsDestination.parquet")) true else false

  private[etl] def getIsFactsDestinationParquet: Boolean = {
    if (configDwEtl.hasPath("dwEtl.dimAuthority.fileDimensionsDestination.parquet")) true else false
  }

  private[etl] def getFactFilePath(factName: String, isForKeySetters: Boolean = false): String = {
    if (getIsFactsDestinationFile) {
      val factNamePatternRegEx = configDwEtl.getString("dwEtl.dataMart.factNamePattern").r
      val factFilePathWithPlaceholder = if (isForKeySetters) {
        val fileSeparator = FileSystems.getDefault().getSeparator()
        val loadTime = new SimpleDateFormat("yyyyMMdd_HH_mm_ss_SSS").format(Calendar.getInstance().getTimeInMillis())
        configDwEtl.getString("dwEtl.dataMart.fileFactsDestination.pathWithKeySetters") + fileSeparator + loadTime + "_" + ModelObject.jobId
      } else {
        configDwEtl.getString("dwEtl.dataMart.fileFactsDestination.path")
      }

      factNamePatternRegEx.replaceAllIn(factFilePathWithPlaceholder, factName)
    }
    else {
      throw new RuntimeException("""Etl Runner ERROR: Fact destination is not a file """)
    }

  }

  /**
   *
   * @return Map[String, (String, EffectiveDateRule, Option[String]) ] - a map of stg source moniker and a 3-tuple with
   *         1. the name of effective date column,
   *            2. the rule type of dates in that source
   *            3. Optional name of the Timestamp column
   */
  private[etl] def getFactProcessMode(factName: String): FactProcessMode = {
    val factConfig = getFactConfiguration(factName)

    if (factConfig.hasPath("processingMode")) {
      factConfig.getString("processingMode") match {
        case "REPLACE" => FactProcessMode.REPLACE
        case "REPLACE_PARTITION" => FactProcessMode.REPLACE_PARTITION
        case "MERGE" => FactProcessMode.MERGE
        case "MERGE_PARTITION" => FactProcessMode.MERGE_PARTITION
        case "ADD" => FactProcessMode.ADD
        case _ => if (getPartitionByCols(factName).isEmpty) FactProcessMode.REPLACE else FactProcessMode.REPLACE_PARTITION
      }
    } else {
      if (getPartitionByCols(factName).isEmpty) FactProcessMode.REPLACE else FactProcessMode.REPLACE_PARTITION // default
    } // _2
  }

  ////////////////////////////////////////////////////////////////////////////
  ////////  Dimensions
  ////////////////////////////////////////////////////////////////////////////

  private[etl] def getDimensionSchema(dimName: String): StructType = super.getSchema(getDimensionConfiguration(dimName), "schema")

  private[etl] def getDimensionSchemaWithoutKey(dimName: String): StructType = {
    val keyColName = getDimensionKeyCol(dimName)

    val dimSchema = getDimensionSchema(dimName)

    val columnsWithNoKey = dimSchema filter {
      case StructField(c, t, _, m) => if (c == keyColName) false else true
    }
    StructType(columnsWithNoKey)
  }

  private[etl] def getDimensionColNamesAndUnknownValues(dimName: String): ListMap[String, String] =
    super.getSchemaColNamesAndPropertyValues(getDimensionConfiguration(dimName), "schema", "unknownValue")

  private[etl] def getDimensionSurrogateKeyForUnknownRow(dimName: String, sep: String = "~"): String = {
    val mapColNamesAndUnknownValues = getDimensionColNamesAndUnknownValues(dimName)
    val mapNaturalKeysAndUnknownValues = mapColNamesAndUnknownValues.filter((colAndValue) => getDimNaturalKeys(dimName).contains(colAndValue._1))
    val surrogateKeyForUnknownRow = mapNaturalKeysAndUnknownValues.values.mkString(sep)
    surrogateKeyForUnknownRow
  }

  private[etl] def getIsDimDestinationFile: Boolean = if (configDwEtl.hasPath("dwEtl.dimAuthority.fileDimensionsDestination.parquet")) true else false

  private[etl] def getIsDimDestinationFileParquet: Boolean = if (getIsDimDestinationFile && configDwEtl.hasPath("dwEtl.dimAuthority.fileDimensionsDestination.parquet")) true else false

  private[etl] def getIsSavePreviousDimVersionOfDestinationFile: Boolean = if (!getIsDimDestinationFile || !configDwEtl.hasPath("dwEtl.dimAuthority.fileDimensionsDestination.previousCopyPath")) false else true

  private[etl] def getDimensionFilePath(dimName: String): String = {
    if (getIsDimDestinationFile) {
      val dimNamePatternRegEx = configDwEtl.getString("dwEtl.dimAuthority.dimensionNamePattern").r
      val dimFilePathWithPlaceholder = configDwEtl.getString("dwEtl.dimAuthority.fileDimensionsDestination.path")

      dimNamePatternRegEx.replaceAllIn(dimFilePathWithPlaceholder, dimName)
    }
    else {
      throw new RuntimeException("""Etl Runner ERROR: Dimension destination is not a file """)
    }

  }

  private[etl] def getDimensionPreviousVersionFilePath(dimName: String): String = {
    if (getIsDimDestinationFile) {
      val dimNamePatternRegEx = configDwEtl.getString("dwEtl.dimAuthority.dimensionNamePattern").r
      val dimFilePathWithPlaceholder = configDwEtl.getString("dwEtl.dimAuthority.fileDimensionsDestination.previousCopyPath")

      dimNamePatternRegEx.replaceAllIn(dimFilePathWithPlaceholder, dimName)
    }
    else {
      throw new RuntimeException("""Etl Runner ERROR: Dimension destination is not a file """)
    }
  }

  /**
   *
   * @param dimName
   * @return the name of the dimension key. The can be only one dimension key column, so return the first one in the list
   */
  private[etl] def getDimensionKeyCol(dimName: String): String =
    super.getSchemaColNamesWithFlag(getDimensionConfiguration(dimName), "schema", "isKey", true, Some(false))(0)

  private[etl] def getDimensionKeyColIndex(dimName: String): Int =
    super.getSchemaColNames(getDimensionConfiguration(dimName), "schema").indexOf(getDimensionKeyCol(dimName))

  private[etl] def getIsDimensionKeySurrogate(dimName: String): Boolean = {
    if (super.getSchemaColNamesWithFlag(getDimensionConfiguration(dimName), "schema", "isSurrogateKey", true, None).isEmpty)
      false
    else
      true
  }

  private[etl] def getDimensionKeyColType(dimName: String): String = {
    val columnNameType = super.getSchemaColNamesAndPropertyValues(getDimensionConfiguration(dimName), "schema", "colType")
    columnNameType(getDimensionKeyCol(dimName))
  }

  private[etl] def getDimNaturalKeys(dimName: String): List[String] =
    super.getSchemaColNamesWithFlag(getDimensionConfiguration(dimName), "schema", "isNaturalKey", true, Some(false))

  private[etl] def getDimCols(dimName: String): List[String] = getDimensionSchema(dimName).fieldNames.toList

  private[etl] def getDimTypeTwoCols(dimName: String): List[String] =
    super.getSchemaColNamesWithFlag(getDimensionConfiguration(dimName), "schema", "isTypeTwo", true, Some(false))

  private[etl] def getDimTypeOneCols(dimName: String): List[String] = {
    super.getSchemaColNames(getDimensionConfiguration(dimName), "schema")
      .diff(List(getDimensionKeyCol(dimName)))
      .diff(getDimNaturalKeys(dimName))
      .diff(getDimTypeTwoCols(dimName))
  }

  private[etl] def getIsEffectiveDateRuleDistinct: Boolean = {
    if (configDwEtl.hasPath("dwEtl.dimAuthority.effectiveDateDays") &&
      configDwEtl.getString("dwEtl.dimAuthority.effectiveDateDays") == "DISTINCT") true else false
  }

  private[etl] def getIsEffectiveDateRuleWeekdays: Boolean = {
    if (configDwEtl.hasPath("dwEtl.dimAuthority.effectiveDateDays") &&
      configDwEtl.getString("dwEtl.dimAuthority.effectiveDateDays") == "WEEKDAYS") true else false
  }

  private[etl] def getIsEffectiveDateRuleAll: Boolean = {
    if ((configDwEtl.hasPath("dwEtl.dimAuthority.effectiveDateDays") &&
      configDwEtl.getString("dwEtl.dimAuthority.effectiveDateDays") == "ALL")
      || !getIsEffectiveDateRuleDistinct || !getIsEffectiveDateRuleWeekdays) true else false
  }

  private[etl] def getIsDimensionsDestinationParquet: Boolean = {
    if (configDwEtl.hasPath("dwEtl.dimAuthority.fileDimensionsDestination.parquet")) true else false
  }

  private[etl] def getStgSourceMonikersOfDim(dimName: String): List[String] = {
    val dimConfig = getDimensionConfiguration(dimName)
    if (!dimConfig.hasPath("dimensionStgSources")) {
      List.empty[String]
    }
    else {
      val dimStgSources = dimConfig.getConfigList("dimensionStgSources").asScala.toList
      dimStgSources map {
        case stgSourceMoniker: Config => {
          stgSourceMoniker.getString("moniker")
        }
      }
    }
  }

  private[etl] def getDimSourceNamesOfDim(dimName: String): List[String] = {
    val dimConfig = getDimensionConfiguration(dimName)
    if (!dimConfig.hasPath("dimDimSources")) {
      List.empty[String]
    }
    else {
      val dimDimSources = dimConfig.getConfigList("dimDimSources").asScala.toList
      dimDimSources map {
        case dimSourceName: Config => dimSourceName.getString("name")
      }
    }
  }

  /**
   * Iterate over all fact tables to be loaded and select all columns from this dimension used to load fact table
   *
   * @param dimName
   * @param dimDimSourceName - dimension that is the source of loading dimName
   * @return - the list of all columns that are used to load dimension.
   */
  private[etl] def getDimensionColsUsedInLoadingDimension(dimName: String, dimDimSourceName: String): List[String] = {

    val dimConfig = getDimensionConfiguration(dimName);

    // get columns from only one this dimension from all dimension sources for this fact table
    val dimensionSourcesToLoadDimensionConfig = dimConfig.getConfigList("dimDimSources").asScala.toList
    val dimensionSourceToLoadDimensionConfig = dimensionSourcesToLoadDimensionConfig find {
      case dim: Config => dim.getString("name") == dimDimSourceName
    }

    // if dimension is in the list it may have a list of columns used in the load of a fact table
    // or the list of columns can be absent in which case all columns will be returned
    val columnsInConfiguration = if (dimensionSourceToLoadDimensionConfig.get.hasPath("schema")) {
      super.getSchemaColNames(getDimensionConfiguration(dimDimSourceName), "schema")
    }
    else // get all columns
    {
      getDimCols(dimDimSourceName)
    }

    columnsInConfiguration
  }

  private[etl] def getDimNamesWhereStgSourceIsUsed(sourceMoniker: String): List[String] = {
    for (dimName <- getDimNamesToLoad if getStgSourceMonikersOfDim(dimName).contains(sourceMoniker)) yield dimName
  }

  ////////////////////////////////////////////////////////////////////////////
  ////////  Dims and Facts
  ////////////////////////////////////////////////////////////////////////////
  private[etl] def getStgSourceMonikersOfModelObject(modelObjectName: String): List[String] = {
    // At most one of these two methods will return a list. The final list may be empty if the object does not have any sources
    val dimNamePatternRegEx = configDwEtl.getString("dwEtl.dimAuthority.dimensionNamePattern").r
    val factNamePatternRegEx = configDwEtl.getString("dwEtl.dataMart.factNamePattern").r

    if (dimNamePatternRegEx.findFirstMatchIn(modelObjectName).isDefined) {
      getStgSourceMonikersOfDim(modelObjectName)
    }
    else if (factNamePatternRegEx.findFirstMatchIn(modelObjectName).isDefined) {
      getStgSourceMonikersOfFact(modelObjectName)
    }
    else {
      throw new RuntimeException("""ETL  ERROR: model object """ + modelObjectName + """does not match a name of a dimension or a fact """)
    }
  }

  ////////////////////////////////////////////////////////////////////////////
  ////////  Stg Sources
  ////////////////////////////////////////////////////////////////////////////
  private[etl] def getSourceMonikers: List[String] = {
    val stgSources = configDwEtl.getConfigList("dwEtl.stgSources").asScala.toList
    stgSources map {
      case source: Config => source.getString("moniker")
    }
  }

  private[etl] def getSourceMonikers( stgSources: List[Config] ): List[String] = {
    stgSources map {
      case source: Config => source.getString("moniker")
    }
  }

  /**
   *
   * @return Map[String, (String, EffectiveDateRule, Option[String]) ] - a map of stg source moniker and a 3-tuple with
   *         1. the name of effective date column,
   *         2. the rule type of dates in that source
   *         3. Optional name of the Timestamp column
   */
  private[etl] def getStgSourceMonikersForDeterminingEffectiveDates(modelObjectName: String): Map[String, (String, EffectiveDateRule, Option[String])] = {
    val stgSources = configDwEtl.getConfigList("dwEtl.stgSources").asScala.toList
    val stgSourceDefaultForEffectiveDate = stgSources filter {
      case source: Config => if (source.hasPath("effectiveDateColumn") && source.hasPath("isDefaultForEffectiveDate") && source.getBoolean("isDefaultForEffectiveDate")) true else false
    }

    val stgSourcesWithEffectiveDateForModelObject = stgSources filter {
      case source: Config => if (source.hasPath("effectiveDateColumn")
        && getStgSourceMonikersOfModelObject(modelObjectName).contains(source.getString("moniker"))) true else false
    }

    val stgSourcesWithEffectiveDate = if (stgSourcesWithEffectiveDateForModelObject.isEmpty) stgSourceDefaultForEffectiveDate else stgSourcesWithEffectiveDateForModelObject


    val stgSourceAndEffectiveDateProperties = stgSourcesWithEffectiveDate map {
      case source: Config =>
        source.getString("moniker") -> (
          source.getString("effectiveDateColumn"), // _1
          if (source.hasPath("effectiveDateRule")) {
            source.getString("effectiveDateRule") match {
              case "DISTINCT" => EffectiveDateRule.DISTINCT_DATES
              case "WEEKDAYS" => EffectiveDateRule.WEEKDAYS
              case _ => EffectiveDateRule.ALL_DATES //  all dates between min and max dates
            }
          } else {
            EffectiveDateRule.ALL_DATES // default
          }, // _2
          if (source.hasPath("timestampColumn")) {
            Some(source.getString("timestampColumn"))
          } else {
            None // default
          } // _3
        )
    }

    stgSourceAndEffectiveDateProperties.toMap
  }

  private[etl] def getStgSourceTimestampColumn(sourceMoniker: String): Option[String] = {
    if (getSourceConfiguration(sourceMoniker).hasPath("timestampColumn")) {
      Some(getSourceConfiguration(sourceMoniker).getString("timestampColumn"))
    } else {
      None
    }
  }

  private def getSourceConfiguration(sourceMoniker: String): Config = {
    val stgSources = configDwEtl.getConfigList("dwEtl.stgSources").asScala.toList
    val stgSource = stgSources find {
      case source: Config => {
        source.getString("moniker") == sourceMoniker
      }
    }
    if (!stgSource.isDefined)
      throw new RuntimeException("""ETL  ERROR: etl source """ + sourceMoniker + """is not defined """)
    stgSource.get
  }

  private[etl] def getIsFileSourceParquet(sourceMoniker: String): Boolean = {
    if (getSourceConfiguration(sourceMoniker).hasPath("fileSource.parquet"))
      true
    else
      false
  }

  private[etl] def getSourceFilePath(sourceMoniker: String): String = {
    getSourceConfiguration(sourceMoniker).getString("fileSource.path")
  }

}
