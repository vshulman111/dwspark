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

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.dbtimes.dw.common._

abstract private[sourceloader] class ActionProcessor {
  private[sourceloader] def process: Unit

  private[sourceloader] def postProcess(action: LoadAction, dfStg: DataFrame): DataFrame = {

    if (!action.getIsPostProcessDefined) {
      dfStg
    }
    else {
      // load all staging sources used in post process
      loadStagingSources(action)  // this will create temp views with same names as staging file monikers,
                                  // so we do not need to save the result of this call
      getPostProcessedStgSource(action, dfStg)
    }
  }

  private def loadStagingSources(action: LoadAction): Map[String, DataFrame] = {
    val list = for (
      stgSourceMoniker <- action.getPostProcessStgSourceMonikers;
      dfStgSource = loadStagingSource(stgSourceMoniker, action)
    ) yield stgSourceMoniker -> dfStgSource

    for ((stgSourceMoniker, dfStgSource) <- list) {
      dfStgSource.createOrReplaceTempView(stgSourceMoniker)
    }

    list.toMap
  }

  private def loadStagingSource(sourceMoniker: String, action: LoadAction): DataFrame = {
    val spark = SparkSession.builder().getOrCreate() // this gets previously created session

    val dfStgSource = if (action.getIsPostProcessFileStgSourceParquet(sourceMoniker)) {
      spark.read.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
        .load(action.getPostProcessStgSourceFilePath(sourceMoniker))
    }
    else
      throw new RuntimeException("""Loader  ERROR: unsupported staging source type  """)

    dfStgSource
  }

  private def getPostProcessedStgSource(action: LoadAction, dfStg: DataFrame): DataFrame = {
    val packageName = action.getPostProcessPackageName
    val moduleName = action.getPostProcessModuleName
    val methodName = action.getPostProcessMethodName

    val invoker = MiscHelper.getInvokerForDynamicMethodInvokation(packageName, moduleName, methodName)

    val applicationSpecificConfigAsOption = action.getPostProcessApplicationSpecific

    // Invoke the method with arguments
    val dfPostProcessed = invoker(applicationSpecificConfigAsOption, dfStg).asInstanceOf[DataFrame]

    dfPostProcessed
  }

}
