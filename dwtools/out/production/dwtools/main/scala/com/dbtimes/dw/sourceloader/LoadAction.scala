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

import java.text.SimpleDateFormat
import java.text.DateFormat
import java.util.Date

import com.typesafe.config.{Config, ConfigValue}
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.types.StructType
import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import com.dbtimes.dw.common._

private[sourceloader] class LoadAction(private val loadAction: ConfigValue) extends ConfigDwJobsCommon with SourceDataAction {

  private val action: Config = loadAction.atKey("action") // giving this object a key allows reference this element as root by name

  /**
   * Validate action against rules in addition to standard schema validation
   */
  private[sourceloader] def validate: Seq[String]  = {
    var errors: mutable.Seq[String] = mutable.Seq.empty[String]

    // check:
    // if partitioning is used and min and max values are not defined in the list of attributes
    // and the type of partitioning column is not integarl type the show an error
    // ( for integral types the min and max value will be calculated in dbms helper code

    val dbmsAttributes: Map[String,String] = getDbmsSpecificAttributes
    if ( getPartitionColumn.isDefined && ( !dbmsAttributes.contains("lowerBound") || !dbmsAttributes.contains("upperBound")) && !isColumnIntegerType( getSchema( getPartitionColumn.get ).dataType ) )
        errors = errors :+ s"""Error in "$getName" load action: partition column "${getPartitionColumn.get}" of non-integral type "${getSchema( getPartitionColumn.get ).dataType.typeName}" is defined, but the database fields does not have "lowerBound" and/or "upperBound"."""

    if ( getPartitionColumn.isDefined && !dbmsAttributes.contains("dbtable") )
      errors = errors :+ s"""Error in "$getName" load action: "dbtable" attribute is required for database attributes when partitioning is used to read data, i.e., when partition column is defined in the schema."""

    /*
      // sample check
    val mergeKeysList = super.getSchemaColNamesWithFlag(action, "action.schema", "isMergeKey", true, Some(false))
    if (!mergeKeysList.isEmpty)
      errors = errors :+ s"""error in '$getName' load action: current version of the loader does not support merge keys."""
    */
    errors.toSeq
  }


  // Methods applicable to all types of sources
  private[sourceloader] def getIsActive: Boolean = action.getBoolean("action.isActive")

  private[sourceloader] def getIsDebugDwLib: Boolean = if (!action.hasPath("action.isDebugDwLib")) false else action.getBoolean("action.isDebugDwLib") // no debug by default

  private[sourceloader] def getName: String = action.getString("action.name")

  private[sourceloader] def getIsRemoveDuplicateRows: Boolean = {
    if (action.hasPath("action.fileSource.isRemoveDuplicateRows") ) {
      action.getBoolean("action.fileSource.isRemoveDuplicateRows")
    }
    else if ( action.hasPath("action.dbmsSource.isRemoveDuplicateRows") ) {
      action.getBoolean("action.dbmsSource.isRemoveDuplicateRows")
    }
    else {
      false
    }
  }

  /***
   * Incremental Load is opposite of Full load. Do not mix Full and Initial loads. Full or Incremental load can be used for subsequent loads
   * @return
   */
  private[sourceloader] def isIncrementalLoad: Boolean = {
    if (!getSdaIsInitialLoad
      && getIsVersioned // for non-versioned data we need to read the full set anyway. For now, we do not support incremental load on non-versioned data.
      && !getSdaMergeKeysList.isEmpty // need merge key to include only non-deleted rows in the new set
      && getIsIncrementalLoadDefined ) true else false
  }

  /**
   * @return - returned schema is for spark consumption with all custom fields, e.g., "isUniqueKey", filtered out
   *         if schema is absent in configuration file - the empty schema is returned
   */
  private[sourceloader] def getSchema: StructType = super.getSchema(action, "action.schema")

  private[sourceloader] def getPrimaryKeysList: List[String] = super.getSchemaColNamesWithFlag(
    action, "action.schema", "isUniqueKey", true, Some(false))

  private[sourceloader] def getExcludeFromVersioningColumnList: List[String] = super.getSchemaColNamesWithFlag(
    action, "action.schema", "isExcludeFromVersioning", true, Some(false))

  // Methods applicable to Load Control configuration
  private[sourceloader] def getIsMaintainLoadControl: Boolean = if (getIsMaintainLoadControlAsParquetFile /* || getIsMaintainLoadControlAsTable */ ) true else false

  private[sourceloader] def getIsMaintainLoadControlAsParquetFile: Boolean = if (action.hasPath("action.fileLoadControl.parquet")) true else false

  private[sourceloader] def getLoadControlParquetFileDir: String = if (getIsMaintainLoadControlAsParquetFile) action.getString("action.fileLoadControl.dir") else throw new RuntimeException("""Loader Configuration ERROR: Load Control is not defined as a parquet file. """)

  // Methods applicable to source
  private[sourceloader] def getIsFileSource: Boolean = if (action.hasPath("action.fileSource")) true else false

  private[sourceloader] def getIsDbmsSource: Boolean = if (action.hasPath("action.dbmsSource")) true else false

  private[sourceloader] def getSourceTypeDescription: String = if (getIsFileSource && getIsFileSourceCsv) "csv file" else if (getIsDbmsSource) "DBMS table" else "Unknown"

  private[sourceloader] def getSourceDescription: String = {
    if (getIsFileSourceFileSystemLocation)
      getSourceFileDir + getSourceFileNamePattern
    else if (getIsFileSourceInternetLocation)
      getSourceFileUrl
    else if (getIsDbmsSource) {
      if ( getSourceTable.isDefined )
        getSourceTable.get
      else if ( getMongoDbCollectionAsOption.isDefined )
        getMongoDbCollectionAsOption.get
      else
        "Unknown dbms source table"
    }
    else
      "Unknown"
  }

  // Methods applicable to file source
  private[sourceloader] def getIsFileSourceFileSystemLocation: Boolean = if (getIsFileSource && action.hasPath("action.fileSource.fileSystemLocation")) true else false
  private[sourceloader] def getIsFileSourceInternetLocation: Boolean = if (getIsFileSource && action.hasPath("action.fileSource.internetLocation")) true else false
  private[sourceloader] def getIsFileSourceCustomLocation: Boolean = if (getIsFileSource && action.hasPath("action.fileSource.customLocation")) true else false

  // Methods applicable to fileSystem location
  private[sourceloader] def getSourceFileDir: String = if (getIsFileSourceFileSystemLocation) action.getString("action.fileSource.fileSystemLocation.dir") else throw new RuntimeException("""Loader Configuration ERROR: "fileSource.fileSystemLocation.dir" is only valid for "fileSource.fileSystemLocation" """)
  private[sourceloader] def getSourceFileNamePattern: String = if (getIsFileSourceFileSystemLocation) action.getString("action.fileSource.fileSystemLocation.namePattern") else throw new RuntimeException("""Loader Configuration ERROR: "fileSource.fileSystemLocation.namePattern" is only valid for "fileSource.fileSystemLocation" """)
  // Methods applicable to internet location
  private[sourceloader] def getSourceFileUrl: String = if (getIsFileSourceInternetLocation) action.getString("action.fileSource.internetLocation.url") else throw new RuntimeException("""Loader Configuration ERROR: "fileSource.internetLocation.url" is only valid for "fileSource.internetLocation" """)
  private[sourceloader] def getSourceFileUser: Option[String] = {
    if (getIsFileSourceInternetLocation)
      if (action.hasPath("action.fileSource.internetLocation.user"))
        Some( action.getString("action.fileSource.internetLocation.user") )
      else
        None
    else
      throw new RuntimeException("""Loader Configuration ERROR: "fileSource.internetLocation.user" is only valid for "fileSource.internetLocation" """)
  }
  private[sourceloader] def getSourceFilePassword: Option[String] = {
    if (getIsFileSourceInternetLocation)
      if (action.hasPath("action.fileSource.internetLocation.password"))
        Some( action.getString("action.fileSource.internetLocation.password") )
      else
        None
    else
      throw new RuntimeException("""Loader Configuration ERROR: "fileSource.internetLocation.password" is only valid for "fileSource.internetLocation" """)
  }
  private[sourceloader] def getIsDisableSslVerification: Option[Boolean] = {
    if (getIsFileSourceInternetLocation)
      if (action.hasPath("action.fileSource.internetLocation.isDisableSslVerification"))
        Some( action.getBoolean("action.fileSource.internetLocation.isDisableSslVerification") )
      else
        None
    else
      throw new RuntimeException("""Loader Configuration ERROR: "isDisableSslVerification" is only valid for "fileSource.internetLocation" """)
  }
  // Methods applicable to custom location
  private[sourceloader] def getSourceFilePackageName: String = {
    if (getIsFileSourceCustomLocation)
      if (action.hasPath("action.fileSource.customLocation.packageName"))
        action.getString("action.fileSource.customLocation.packageName")
      else
        ""
    else
      throw new RuntimeException("""Loader Configuration ERROR: "fileSource.customLocation.packageName" is only valid for "fileSource.customLocation" """)
  }
  private[sourceloader] def getSourceFileModuleName: String = if (getIsFileSourceCustomLocation) action.getString("action.fileSource.customLocation.moduleName") else throw new RuntimeException("""Loader Configuration ERROR: "fileSource.customLocation.moduleName" is only valid for "fileSource.customLocation" """)
  private[sourceloader] def getSourceFileMethodName: String = if (getIsFileSourceCustomLocation) action.getString("action.fileSource.customLocation.methodName") else throw new RuntimeException("""Loader Configuration ERROR: "fileSource.customLocation.methodName" is only valid for "fileSource.customLocation" """)
  private[sourceloader] def getIsSourceFileMethodReturnsFilePath: Boolean = {
    if (getIsFileSourceCustomLocation) {
      val methodReturns = action.getString("action.fileSource.customLocation.methodReturns")
      if ( methodReturns == "FilePath" )
        true
      else
        false
    }
    else
      throw new RuntimeException("""Loader Configuration ERROR: "fileSource.customLocation.methodReturns" is only valid for "fileSource.customLocation" """)
  }
  private[sourceloader] def getIsSourceFileMethodReturnsFileData: Boolean = {
    if (getIsFileSourceCustomLocation) {
      val methodReturns = action.getString("action.fileSource.customLocation.methodReturns")
      if ( methodReturns == "FileData" )
        true
      else
        false
    }
    else
      throw new RuntimeException("""Loader Configuration ERROR: "fileSource.customLocation.methodReturns" is only valid for "fileSource.customLocation" """)
  }
  private[sourceloader] def getSourceFileApplicationSpecific: Option[Config] = {
    if (getIsFileSourceCustomLocation)
      if (action.hasPath("action.fileSource.customLocation.applicationSpecific"))
        Some( action.getConfig("action.fileSource.customLocation.applicationSpecific") )
      else
        None
    else
      throw new RuntimeException("""Loader Configuration ERROR: "fileSource.internetLocation.applicationSpecific" is only valid for "fileSource.internetLocation" """)
  }

  // Methods applicable to all locations of file source
  private[sourceloader] def getProcessedFilesDir: Option[String] = {
    if (getIsFileSource)
      if (action.hasPath("action.fileSource.processedFilesDir"))
        Some(action.getString("action.fileSource.processedFilesDir"))
      else
        None
    else
      throw new RuntimeException("""Loader Configuration ERROR: "fileSource.processedFilesDir" is only valid for "fileSource" """)
  }

  private[sourceloader] def getIsFileSourceCsv: Boolean = if (getIsFileSource && action.hasPath("action.fileSource.csv")) true else false

  //
  // Methods applicable to file source - csv
  // -----------------------------------------
  private[sourceloader] def getCsvOptions: Map[String,String] = {
    super.getConfigObjectFieldsAndValues( if (getIsFileSourceCsv) Some( action.getConfig("action.fileSource.csv") ) else None )
  }

  //
  // Methods applicable to dbms source
  // -----------------------------------
  private[sourceloader] def getConnectionUri: Option[String] = {
    if (getIsDbmsSource ) {
      val dbmsAttributes: Map[String,String] = getDbmsSpecificAttributes

      if ( ( getIsSqlServerSource || getIsOracleSource) && dbmsAttributes.contains("url") ) {
        dbmsAttributes.get("url")
        }
        else if ( getIsMongoDbSource && dbmsAttributes.contains("connection.uri") ) {
          dbmsAttributes.get("connection.uri")
        }
        else {
          None
        }
    }
    else
      None
  }

  private[sourceloader] def getUser: Option[String] = {
    if (getIsDbmsSource && action.hasPath("action.dbmsSource.user"))
      Some(action.getString("action.dbmsSource.user"))
    else
      None
  }

  private[sourceloader] def getPassword: Option[String] = {
    if (getIsDbmsSource && action.hasPath("action.dbmsSource.password"))
      Some(action.getString("action.dbmsSource.password"))
    else
      None
  }

  private[sourceloader] def getSourceTable: Option[String] = {
    if (getIsDbmsSource ) {
      val dbmsAttributes: Map[String,String] = getDbmsSpecificAttributes

      if ( ( getIsSqlServerSource || getIsOracleSource) && dbmsAttributes.contains("dbtable") ) {
        getDbmsSpecificAttributes.get("dbtable")
      }
      else if ( getIsMongoDbSource && dbmsAttributes.contains("collection") ) {
        getDbmsSpecificAttributes.get("collection")
      }
      else {
        None
      }
    }
    else
      None
  }

  private[sourceloader] def getPartitionColumn: Option[String] = {
    if ( !action.hasPath("action.schema") )
      None
    else {
      val partitionColumns = super.getSchemaColNamesWithFlag(
        action, "action.schema", "isPartitionColumn", true, Some(false))
      if ( partitionColumns.isEmpty)
        None
      else
        Some( partitionColumns.head )
    }
  }

  private[sourceloader] def getNumberOfPartitions: Int = {
    if (getIsDbmsSource ) {
      val dbmsAttributes: Map[String,String] = getDbmsSpecificAttributes

      if ( ( getIsSqlServerSource || getIsOracleSource) && dbmsAttributes.contains("numPartitions") ) {
        dbmsAttributes.get("numPartitions").get.toInt
      }
      else {
        1 // default number of partitions
      }
    }
    else
      1 // default number of partitions
  }

  private[sourceloader] def getIsSqlServerSource: Boolean = if (getIsDbmsSource && action.hasPath("action.dbmsSource.sqlServer")) true else false
  private[sourceloader] def getIsOracleSource: Boolean = if (getIsDbmsSource && action.hasPath("action.dbmsSource.oracle")) true else false
  private[sourceloader] def getIsMongoDbSource: Boolean = if (getIsDbmsSource && action.hasPath("action.dbmsSource.mongoDb")) true else false

  private[sourceloader] def getMongoDbCollectionAsOption: Option[String] = {
    if (getIsMongoDbSource) {
      if (action.hasPath("action.dbmsSource.mongoDb.collection"))
        Some(action.getString("action.dbmsSource.mongoDb.collection"))
      else if (action.hasPath("""action.dbmsSource.mongoDb."spark.mongodb.read.collection""""))
        Some(action.getString("""action.dbmsSource.mongoDb."spark.mongodb.read.collection""""))
      else
        None
    }
    else
      None
  }
  private[sourceloader] def getDbmsSpecificAttributes: Map[String,String] = {
    if ( getIsSqlServerSource )
      super.getConfigObjectFieldsAndValues( Some( action.getConfig("action.dbmsSource.sqlServer") ) )
    else if ( getIsOracleSource )
      super.getConfigObjectFieldsAndValues( Some( action.getConfig("action.dbmsSource.oracle") ) )
    else if ( getIsMongoDbSource )
      super.getConfigObjectFieldsAndValues( Some( action.getConfig("action.dbmsSource.mongoDb") ) )
    else
      Map[String,String] ()
  }


  // Methods applicable to destination
  private[sourceloader] def getIsVersioned: Boolean = if (!action.hasPath("action.fileDestination.isVersioned")) {
    true // Versioned by default for both cases: with unique key and with no unique key
  }
  else {
    action.getBoolean("action.fileDestination.isVersioned")
  }

  private[sourceloader] def getDestinationTypeDescription: String = if (getIsFileDestinationParquet) "parquet file" else "Unknown" // Add DBMS details when implemented
  private[sourceloader] def getDestinationDescription: String = if (getIsFileDestinationParquet) getDestinationFilePath else "Unknown" // Add DBMS details when implemented
  private[sourceloader] def getIsFileDestination: Boolean = if (action.hasPath("action.fileDestination")) true else false

  private[sourceloader] def getIsDbmsDestination: Boolean = if (action.hasPath("action.dbmsDestination")) true else false

  private[sourceloader] def getIsFileDestinationParquet: Boolean = if (getIsFileDestination && action.hasPath("action.fileDestination.parquet")) true else false

  private[sourceloader] def getDestinationFilePath: String = if (getIsFileDestination) action.getString("action.fileDestination.path") else throw new RuntimeException("""Loader Configuration ERROR: Destination is not a file """)

  private[sourceloader] def getIsSavePreviousVersionOfDestinationFile: Boolean = if (!getIsFileDestination || !action.hasPath("action.fileDestination.previousCopyPath")) false else true

  private[sourceloader] def getFileDestinationSavePreviousVersionAs: String = if (getIsSavePreviousVersionOfDestinationFile) action.getString("action.fileDestination.previousCopyPath") else throw new RuntimeException("""Loader Configuration ERROR: "fileSource.previousCopyPath" is not specified """)

  // Effective date methods
  private def getEffectiveDateColumn: Option[String] = None

  private[sourceloader] def getEffectiveDate(filePath: String): Option[Date] = { // Extracts Effective date from file name
    val fileName = FilenameUtils.getName(filePath.toString)
    // try to extract the effective date from the file name using the pattern that name can have Named Capturing Group
    // e.g., pattern pbp-2015_(?<effectiveDate>(?<year>20[0-9][0-9])-(?<month>[0-1][0-9])-(?<day>[0-3][0-9])).*\.csv
    // will match files pbp-2015_2020-03-21.csv or pbp-2015_2020-12-03 added missed plays.csv
    val fileNamePattern = getSourceFileNamePattern.r // pattern to regular expression
    val sequence = fileNamePattern.findAllMatchIn(fileName).toSeq

    // `sequence.length` breaks the underlying Match object
    //println(sequence.length)
    val matched = sequence.head

    /* this syntax to extract parts of the effective date does not work in the current version of Scala. Do it the hard way to
    see the names of Capturing groups and their order in the file pattern
    val year = matched.group("year")  // year does not have a default - it must be specified
    val month = matched.group("month") // default month to 01
    val day = matched.group("day").toInt      // default day to 01
     */
    // identify which Named Capture groups patterns are specified and extract them using ordinal
    // assume that the only named capture groups in the file name are
    // ?<effectiveDate> - must be present to extract effective date from the file name
    // ?<year> - must be present. Can be in YY or YYYY form
    // ?<month> - optional. Value 1 to 12. Defaul: 1
    // ?<day> - optional. Value 1 to 31. Default: 1
    val knownCaptureGroupNames = List("effectiveDate", "year", "month", "day")

    // Find Which of these are present in a given file pattern
    val positionsOfEffDateNamedGroups = knownCaptureGroupNames map {
      case knownCaptureGroupName: String => {
        val captureGroupName = "?<" + knownCaptureGroupName + ">"
        (getSourceFileNamePattern.indexOf(captureGroupName), knownCaptureGroupName) // e.g., (0,"effectiveDate" ), (1, "year), etc.
      }
    }

    val effDateNamedGroups = positionsOfEffDateNamedGroups filter (_._1 > 0) // filter out the parts that are not present in the file name pattern
    val effDateNamedGroupsOrderedByPosition = effDateNamedGroups.sortWith(_._1 < _._1)

    val effectiveDate = if (effDateNamedGroupsOrderedByPosition.find(x => x._2 == "effectiveDate").isEmpty)
      None // this is the result. It means that effective date is not specified in the file pattern
    else { // we have "effectiveDate" present in the pattern. Try to construct a date
      val year = if (effDateNamedGroupsOrderedByPosition.find(x => x._2 == "year").isEmpty) // year does not have a default - it must be specified
        throw new RuntimeException("""Loader Configuration ERROR: effectiveDate does not have ?<year> Named Capture Group defined in a file name pattern """)
      else {
        val ordinalPosOfYear = effDateNamedGroupsOrderedByPosition.indexOf(effDateNamedGroupsOrderedByPosition.find(x => x._2 == "year").get) + 1
        matched.group(ordinalPosOfYear).toInt // match number for Named Capture Group is 1-based because index 0 is the the entire string
      }

      val month = if (effDateNamedGroupsOrderedByPosition.find(x => x._2 == "month").isEmpty)
        1 // default month to 1
      else
        matched.group(effDateNamedGroupsOrderedByPosition.indexOf(effDateNamedGroupsOrderedByPosition.find(x => x._2 == "month").get) + 1).toInt // match number for Named Capture Group is 1-based because index 0 is the the entire string

      val day = if (effDateNamedGroupsOrderedByPosition.find(x => x._2 == "day").isEmpty)
        1 // default day to 1
      else
        matched.group(effDateNamedGroupsOrderedByPosition.indexOf(effDateNamedGroupsOrderedByPosition.find(x => x._2 == "day").get) + 1).toInt // match number for Named Capture Group is 1-based because index 0 is the the entire string

      val yearYYYY = if (year >= 0 && year < 50)
        2000 + year
      else if (year >= 50 && year < 100)
        1900 + year
      else
        year

      val date_YYYYMMDD = f"$yearYYYY%04d$month%02d$day%02d" // pad with zeros

      Some(new SimpleDateFormat("yyyyMMdd").parse(date_YYYYMMDD))
    }

    effectiveDate
  }

  private[sourceloader] def getEffectiveDate(srcDataFrame: DataFrame, filePath: String = ""): Date = { // the file name can be omitted for non-file sources like dbms source, in which case it is not used to get effective date
    // Effective date can be fixed date, derived from the file name, a column in the data or today
    // Effective dates are retrieved in the order of precedence, so if more than one specified the
    // one with higher precedence will be used
    if (action.hasPath("action.fileSource.effectiveDate")) // Effective Date is hardcoded
      new SimpleDateFormat("yyyy-MM-dd").parse(action.getString("action.fileSource.effectiveDate")) // the explicit effective dte must have format yyyy-MM-dd
    else if (action.hasPath("action.dbmsSource.effectiveDate")) // Effective Date is hardcoded
      new SimpleDateFormat("yyyy-MM-dd").parse(action.getString("action.dbmsSource.effectiveDate")) // the explicit effective dte must have format yyyy-MM-dd
    else if (getEffectiveDateColumn.isDefined)
      throw new RuntimeException("""Loader Configuration ERROR: effectiveDate column property is not yet handled """)
    else if (!filePath.isEmpty && getIsFileSourceFileSystemLocation && (getEffectiveDate(filePath).isDefined))
      getEffectiveDate(filePath).get
    else
      new Date() // for now just return today
  }

  private[sourceloader] def getIsIncrementalLoadDefined: Boolean = if (action.hasPath("action.dbmsSource.incrementalLoad")) true else false

  private[sourceloader] def getIncrementalLoadSourceTableChangesSql: String = {
    val dbmsAttributes: Map[String,String] = getDbmsSpecificAttributes

    val sourceTableChangesSql = {
      if ( !getIsIncrementalLoadDefined ) {
        ""
      }
      else if ( ( getIsSqlServerSource || getIsOracleSource) && dbmsAttributes.contains("dbtable") ) {
        getDbmsSpecificAttributes.get("dbtable").get
      }
      else if ( getIsMongoDbSource && dbmsAttributes.contains("aggregation.pipeline") ) {
        getDbmsSpecificAttributes.get("aggregation.pipeline").get
      }
      else {
        ""
      }
    }

    sourceTableChangesSql
  }

  private[sourceloader] def getIncrementalLoadUniqueKeysSql: Option[ String ] = {
    val uniqueKeysSql = {
      if ( !getIsIncrementalLoadDefined ) {
        None
      }
      else if ( action.hasPath("action.dbmsSource.incrementalLoad.uniqueKeys") ) {
        Some( action.getString("action.dbmsSource.incrementalLoad.uniqueKeys") )
      }
      else {
        None
      }
    }
    uniqueKeysSql
  }

  private[sourceloader] def getIncrementalLoadColumnNameAndPlaceholderPairs: List[(String, String)] = {

    val sourceTableChangesSql = getIncrementalLoadSourceTableChangesSql

    if ( getIsIncrementalLoadDefined && !sourceTableChangesSql.isEmpty) {
      // the pattern will look something like this
      // "(?<delimStart>:::)(?<column>.+)(?<delimEnd>:::)"
      // where ::: is the delimiter for max value fields
      //  and column between delimiters is the column in the destination file
      val watermarkColumnsPattern =
        ( "(?<delimStart>" +
          action.getString("action.dbmsSource.incrementalLoad.delimForWatermarkColumns") +
          ")(?<column>.+)(?<delimEnd>" +
          action.getString("action.dbmsSource.incrementalLoad.delimForWatermarkColumns") +
          ")" ).r

      val allPlaceholderMatches = watermarkColumnsPattern.findAllMatchIn(sourceTableChangesSql).toSeq
      val columns = (for (placeholder <- allPlaceholderMatches) yield placeholder.group(2)).toList // result has a list of columns, e.g., List[String] = List(RowVersion, RowVersion2). Also, placeholder.group( "column" )does not work for older version of scala. Using ordinal value
      val placeholders = (for (placeholder <- allPlaceholderMatches) yield placeholder.toString()).toList // result like this List(<@RowVersion@>, <@RowVersion2@>)

      columns zip placeholders // List((RowVersion, :::RowVersion::: ), (RowVersion2, :::RowVersion2::: ))
    } else {
      List.empty[(String, String)]
    }
  }

  //def getIsFullLoad: Boolean = true

  //def getCanDeleteKeysOnIncrementalLoad: Boolean = false

  // Implement SourceDataAction trait
  override private[dw] def getSdaName: String = getName

  override private[dw] def getSdaIsInitialLoad: Boolean = {
    val spark = SparkSession.builder().getOrCreate() // this gets previously created session
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    !fs.exists(new HadoopPath(getDestinationFilePath))
  }

  override private[dw] def getSdaDestinationFilePath: String = getDestinationFilePath

  override private[dw] def getSdaPrimaryKeysList: List[String] = getPrimaryKeysList

  override private[dw] def getSdaExcludeFromVersioningColumnList: List[String] = getExcludeFromVersioningColumnList

  override private[dw] def getSdaMergeKeysList: List[String] = {
/*
    val mergeKeysList = super.getSchemaColNamesWithFlag(action, "action.schema", "isMergeKey", true, Some(false))
    if (!mergeKeysList.isEmpty && mergeKeysList != getPrimaryKeysList) {
      throw new RuntimeException("""Loader Configuration ERROR: current version of the loader does not support merge keys different from unique keys """)
    }
*/
    // For now just return the same keys as Primary keys. In case Merge keys are not set initially and
    // then incremental load is run. Otherwise, in this scenario the incremental load will not work since
    // merge keys column will be null from initial load.
    getPrimaryKeysList
  }

  override private[dw] def getSdaIsMaintainLoadControlAsParquetFile: Boolean = getIsMaintainLoadControlAsParquetFile

  override private[dw] def getSdaIsFileDestination: Boolean = getIsFileDestination

  override private[dw] def getSdaIsDbmsDestination: Boolean = getIsDbmsDestination

  override private[dw] def getSdaIsFileDestinationParquet: Boolean = getIsFileDestinationParquet

  override private[dw] def getSdaIsVersioned: Boolean = getIsVersioned

  override private[dw] def getSdaIsDebugDwLib: Boolean = getIsDebugDwLib

  override private[dw] def getSdaIsSavePreviousVersionOfDestinationFile: Boolean = getIsSavePreviousVersionOfDestinationFile

  override private[dw] def getSdaFileDestinationSavePreviousVersionAs: String = getFileDestinationSavePreviousVersionAs

  override private[dw] def getSdaSourceTypeDescription: String = getSourceTypeDescription

  override private[dw] def getSdaSourceDescription: String = getSourceDescription

  override private[dw] def getSdaLoadControlParquetFileDir: String = getLoadControlParquetFileDir

  override private[dw] def getSdaIsRemoveDuplicateRows: Boolean = getIsRemoveDuplicateRows

  override private[dw] def getSdaServiceColumnPrefix: String = "_SrcDt_" // so the name doesnot collide with the data

}
