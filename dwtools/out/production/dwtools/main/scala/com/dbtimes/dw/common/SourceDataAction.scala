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

private[dw] trait SourceDataAction {
  private[dw] def getSdaName: String = ""

  private[dw] def getSdaIsInitialLoad: Boolean

  private[dw] def getSdaDestinationFilePath: String

  private[dw] def getSdaPrimaryKeysList: List[String]

  private[dw] def getSdaExcludeFromVersioningColumnList: List[String]

  private[dw] def getSdaMergeKeysList: List[String]

  private[dw] def getSdaIsMaintainLoadControlAsParquetFile: Boolean

  private[dw] def getSdaIsMaintainLoadControl: Boolean = if (getSdaIsMaintainLoadControlAsParquetFile /* || getIsMaintainLoadControlAsTable */ ) true else false

  private[dw] def getSdaIsFileDestination: Boolean

  private[dw] def getSdaIsDbmsDestination: Boolean

  private[dw] def getSdaIsFileDestinationParquet: Boolean

  private[dw] def getSdaIsVersioned: Boolean

  private[dw] def getSdaIsDebugDwLib: Boolean

  private[dw] def getSdaDestinationTypeDescription: String = if (getSdaIsFileDestinationParquet) "parquet file" else "Unknown" // Add DBMS details when implemented
  private[dw] def getSdaDestinationDescription: String = if (getSdaIsFileDestinationParquet) getSdaDestinationFilePath else "Unknown" // Add DBMS details when implemented
  private[dw] def getSdaIsSavePreviousVersionOfDestinationFile: Boolean

  private[dw] def getSdaFileDestinationSavePreviousVersionAs: String

  private[dw] def getSdaSourceTypeDescription: String

  private[dw] def getSdaSourceDescription: String

  private[dw] def getSdaLoadControlParquetFileDir: String

  private[dw] def getSdaIsRemoveDuplicateRows: Boolean

  private[dw] def getSdaServiceColumnPrefix: String
}
