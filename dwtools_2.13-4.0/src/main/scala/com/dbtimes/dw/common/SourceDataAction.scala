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

import scala.util.matching.Regex

// SourceDataAction properties are use in SourceDataMerger which is called in source Load and Etl Fact table processing.
private[dw] trait SourceDataAction {
  private[dw] def getName: String = ""
  private[dw] def getIsInitialLoad: Boolean
  private[dw] def getDestinationFilePath: String
  private[dw] def getPrimaryKeysList: List[String]
  private[dw] def getExcludeFromVersioningColumnList: List[String]
  private[dw] def getMergeKeysList: List[String]
  private[dw] def getIsMaintainLoadControlAsParquetFile: Boolean
  private[dw] def getIsMaintainLoadControl: Boolean = if (getIsMaintainLoadControlAsParquetFile /* || getIsMaintainLoadControlAsTable */ ) true else false
  private[dw] def getIsFileDestination: Boolean
  private[dw] def getIsDbmsDestination: Boolean
  private[dw] def getIsFileDestinationParquet: Boolean
  private[dw] def getIsVersioned: Boolean
  private[dw] def getIsDebugDwLib: Boolean
  private[dw] def getDestinationTypeDescription: String = if (getIsFileDestinationParquet) "parquet file" else "Unknown" // Add DBMS details when implemented
  private[dw] def getDestinationDescription: String = if (getIsFileDestinationParquet) getDestinationFilePath else "Unknown" // Add DBMS details when implemented
  private[dw] def getIsSavePreviousVersionOfDestinationFile: Boolean
  private[dw] def getFileDestinationSavePreviousVersionAs: String
  private[dw] def getSourceTypeDescription: String
  private[dw] def getSourceDescription: String
  private[dw] def getLoadControlParquetFileDir: String
  private[dw] def getIsRemoveDuplicateRows: Boolean
  private[dw] def getMetadataColumnPrefix: String

  // Schema evolution methods
  private[dw] def getIsDefinedSchemaEvolution: Boolean = false
  private[dw] def getIsAllowMetadataColumnsPrefixChange: Boolean = false
  private[dw] def getIsAllowColumnRename: Boolean = false
  private[dw] def getIsAllowColumnAdd: Boolean = false
  private[dw] def getIsAllowColumnDelete: Boolean = false
  private[dw] def getIsAllowColumnTypeChange: Boolean = false
  private[dw] def getColumnsToRename:  Map[String,String] = Map.empty[String, String] // ( fromColumnName, toColumnName )
  // ( columnName, backfillValue for one of the types ["string", "number", "integer", "boolean"])
  // At most one of the four values will be Some, all others will be None
  // If all values in this tuple are None, the backfill value is null
  // If this Sequence is empty - any column can be added with backfill value null
  private[dw] def getColumnsToAdd:  Map[Regex, (Option[String], Option[Double], Option[Long], Option[Boolean] ) ] = Map.empty[ Regex, (Option[String], Option[Double], Option[Long], Option[Boolean] ) ]
  private[dw] def getBackfillValueForStrings: Option[String] = None
  private[dw] def getBackfillValueForFloats: Option[Double] = None
  private[dw] def getBackfillValueForIntegers: Option[Long] = None
  private[dw] def getBackfillValueForBooleans: Option[Boolean] = None
  // If this Sequence is empty and column deletion is allowed - any column can be deleted
  private[dw] def getColumnsToDelete:  Seq[Regex] = Seq.empty[Regex]
  // If this Sequence is empty and column type change is allowed - any column's type can be changed
  private[dw] def getColumnsToChangeType:  Seq[Regex] = Seq.empty[Regex]
}
