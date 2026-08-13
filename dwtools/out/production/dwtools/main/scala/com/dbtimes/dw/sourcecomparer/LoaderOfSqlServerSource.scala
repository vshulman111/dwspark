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

import com.dbtimes.dw.common._
import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

import LogFile.{logger => loaderLog}

private[sourcecomparer] class LoaderOfSqlServerSource(private val scenario: ScenarioConfig, private val isLeftSource: Boolean) extends ScenarioSourceLoader {

  private[sourcecomparer] def loadSource( ): DataFrame = {
    val df = DataFrameHelper.readDfFromSqlServer(
      scenario.getUser(isLeftSource),
      scenario.getPassword(isLeftSource),
      scenario.getConnectionUri(isLeftSource),
      scenario.getSourceTable(isLeftSource),
      scenario.getPartitionColumn(isLeftSource),
      scenario.getNumberOfPartitions(isLeftSource),
      scenario.getDbmsSpecificAttributes(isLeftSource),
      StructType(Seq()) )   // do not use schema for SQL server because datatype inference is good and because of that the schema is either absent or incomplete in the config file

    createSubsetToCompare( scenario, isLeftSource, df )
  }
}
