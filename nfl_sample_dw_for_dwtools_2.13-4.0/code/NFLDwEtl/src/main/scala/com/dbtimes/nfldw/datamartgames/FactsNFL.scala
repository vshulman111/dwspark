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
package com.dbtimes.nfldw.datamartgames

import com.dbtimes.dw.etl.Fact
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession


/**
 * The purpose of this class is to provide some common DataMart functionality
 *
 * @param factName
 */
abstract class FactsNFL(factName: String) extends Fact(factName) {

  protected val playExtraPoint = "EXTRA POINT"
  protected val playExtraPointIsGood = "EXTRA POINT IS GOOD"
  protected val playFieldGoal = "FIELD GOAL"
  protected val playFieldGoalIsGood = "FIELD GOAL IS GOOD"

  /**
   * This is the function to create custom list of dates to process
   *
   * @return
   */
  override def getCustomDatesToProcess(dfDatesToProcessBasedOnConfig: DataFrame): Option[DataFrame] = {

    dfDatesToProcessBasedOnConfig.createOrReplaceTempView("DatesToProcessBasedOnConfig")

    val sqlDatesToProcess
    =
      s"""|SELECT MAX( $effDateColumnNameInDatesToProcess ) AS SeasonYearDate
          |FROM DatesToProcessBasedOnConfig
          |GROUP BY udfSeasonYear( $effDateColumnNameInDatesToProcess )
          |""".stripMargin

    val spark = SparkSession.builder().getOrCreate()
    val dfDatesToProcess = spark.sql(sqlDatesToProcess)

    Some(dfDatesToProcess)
  }

}
