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
// import scala.util.chaining._    // Scala 2.13
import org.apache.spark.sql.{DataFrame, SparkSession}
import LogFile.{dwlogger => comparerLog}

private[sourcecomparer] class LoaderOfCsvFileSource(private val scenario: ScenarioConfig, private val isLeftSource: Boolean) extends ScenarioSourceLoader {

  private[sourcecomparer] def loadSource( ): DataFrame = {
    val spark = SparkSession.builder().getOrCreate() // this gets previously created session
    val sourceFilePath = scenario.getSourceFilePath( isLeftSource )

    val csvOptions = scenario.getCsvOptions( isLeftSource )

    comparerLog.info("** Processing .CSV file " + sourceFilePath)

    // Scala 2.13
/*    val df = spark
      .read.format("com.databricks.spark.csv")
      .options( csvOptions )
      .pipe(q => if ( !(csvOptions.contains("inferSchema") && csvOptions.get("inferSchema").get.toBoolean == true) ) q.schema(scenario.getSchema(isLeftSource)) else q) // The optional call
      .load(sourceFilePath)*/

    // For Scala 2.12 just do it as an if
    val df = if ( !(csvOptions.contains("inferSchema") && csvOptions("inferSchema").toBoolean == true) )
      spark
        .read.format("com.databricks.spark.csv")
        .options( csvOptions )
        .schema(scenario.getSchema(isLeftSource))
        .load(sourceFilePath)
    else
    spark
      .read.format("com.databricks.spark.csv")
      .options( csvOptions )
      .load(sourceFilePath)


    if (scenario.getIsDebugDwLib) {
      df.show(60)
    }

    createSubsetToCompare( scenario, isLeftSource, df )
  }
}
