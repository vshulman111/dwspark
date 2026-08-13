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
package com.dbtimes.dw.sourcecomparer

import org.apache.spark.sql.{DataFrame, SparkSession}

import com.dbtimes.dw.common.LogFile
import LogFile.{dwlogger => comparerLog}

abstract private[sourcecomparer] class ScenarioSourceLoader {
  private[sourcecomparer] def loadSource(): DataFrame

  protected def createSubsetToCompare( scenario: ScenarioConfig, isLeftSource: Boolean, dfBase: DataFrame ): DataFrame = {
    val spark = SparkSession.builder().getOrCreate() // this gets previously created session
    if( scenario.getSubsetQueryToCreateSubsetToCompare( isLeftSource ).isDefined ) {

      val viewNameBaseDf = "WithUniqueKey_" + java.util.UUID.randomUUID.toString.replace("-", "_")
      dfBase.createOrReplaceTempView( viewNameBaseDf )

      val sqlQueryToCreateSubsetToCompare = scenario.getSubsetQueryToCreateSubsetToCompare( isLeftSource ).get.replace( "___", viewNameBaseDf )
      comparerLog.debug(s"  Creating subset data frame for actual compare of ${scenario.getMoniker(isLeftSource)} data source using sql:\n" + sqlQueryToCreateSubsetToCompare )
      val dfToCompare = spark.sql(sqlQueryToCreateSubsetToCompare)
      if (scenario.getIsDebugDwLib) {
        dfToCompare.show(3)
      }
      dfToCompare
    }
    else
      dfBase
  }
}
