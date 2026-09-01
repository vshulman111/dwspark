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
}
