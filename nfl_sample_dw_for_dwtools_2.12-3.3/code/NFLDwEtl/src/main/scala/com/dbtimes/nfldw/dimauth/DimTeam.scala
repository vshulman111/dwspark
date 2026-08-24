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
package com.dbtimes.nfldw.dimauth

import org.apache.spark.sql.DataFrame

import com.dbtimes.dw.etl.Dim
import com.dbtimes.nfldw.DwNFL
import org.apache.spark.sql.SparkSession

/***
 *
 * @param dimName - dimName is a class parameter because it does not have val or var before it's name
 *                in Dim class definition dimName is prefixed with val which makes it a field.
 */
class DimTeam(dimName: String) extends Dim(dimName) {

  override def loadDim(effDateYYYY_MM_DD: String, stgSrcViewWithMonikerName: String): Option[DataFrame] = {
    println(s"-- Processing effective date $effDateYYYY_MM_DD for dimension $dimName from source $stgSrcViewWithMonikerName")

    val dfStgSource = if (stgSrcViewWithMonikerName == "Teams") {
      val sqlStgSource
      =
        s"""| SELECT
            |   TeamName                AS TeamOriginalName,
            |   SeasonTeamName          AS TeamName,
            | --  SeasonTeamName          AS TeamNameType0,
            | --  SeasonTeamName          AS TeamNameType3,
            |   SeasonAbbreviation      AS TeamAbbrevName,
            |   ''                      AS TeamConference,
            |   SeasonTeamName          AS HistTeamName,
            |   SeasonAbbreviation      AS HistTeamAbbrevName
            | FROM $stgSrcViewWithMonikerName
            | WHERE SeasonYear = udfSeasonYear( CAST( '$effDateYYYY_MM_DD' AS DATE) )
             """.stripMargin

      println(s"-- Loading dimension $dimName from source $stgSrcViewWithMonikerName using sql:\n$sqlStgSource")
      val spark = SparkSession.builder().getOrCreate()
      spark.sql(sqlStgSource)
    }
    else
      throw new RuntimeException(s"""Dim Etl ERROR: Unknown stg source while loading dimension $dimName""")

    if (DwNFL.getIsDebug) {
      dfStgSource.show(5);
      dfStgSource.printSchema()
    }

    Some(dfStgSource)
  }

  /**
   * This is the function to create custom list of dates to process
   *
   * @return
   */
  override def getCustomDatesToProcess(dfDatesToProcessBasedOnConfig: DataFrame): Option[DataFrame] = {
    // For DimTeans we need one date per  season
    dfDatesToProcessBasedOnConfig.createOrReplaceTempView("DatesToProcessBasedOnConfig")

    if (DwNFL.getIsDebug) {
      dfDatesToProcessBasedOnConfig.show(50);
      dfDatesToProcessBasedOnConfig.printSchema()
    }

    val sqlDimDatesToProcess
    =
      s"""|SELECT MAX( datesAll.$effDateColumnNameInDatesToProcess ) AS SeasonDate
          |FROM Teams
          |   INNER JOIN DatesToProcessBasedOnConfig AS datesAll ON Teams.SeasonYear = udfSeasonYear( datesAll.$effDateColumnNameInDatesToProcess )
          |GROUP BY Teams.SeasonYear
					|  """.stripMargin
    val spark = SparkSession.builder().getOrCreate()
    val dfDimDatesToProcess = spark.sql(sqlDimDatesToProcess)

    if (DwNFL.getIsDebug) {
      dfDimDatesToProcess.show(5);
      dfDimDatesToProcess.printSchema()
    }

    Some(dfDimDatesToProcess)
  }

  override def setForeignKeyOnFactTable(fact: DataFrame, mapDimColsToFactCols: Map[String, String], effDateColAsOption: Option[String]): DataFrame = {
    // If we are here it means that this method was not overridden in the actual dimension.
    // Make sure we only have Natural Key in the factColumnsToDimColsMap and we have all the natural keys
    println(s""" -- Setting keys on fact table from dimension $dimName using custom override method """)

    val viewFactTable = s"factWhenSettingKeyFrom$dimName"
    fact.createOrReplaceTempView(viewFactTable)

    //  There is a view created with the same name as dimension
    // We use NULL safe join as if both columns are NULLs this is a good key
    // The code assumes that there is only one key-value pair in the mapDimColsToFactCols
    val sqlToSetKey =
    s"""
       |SELECT fact.*, IFNULL( dim.$dimKey, $dimKeyUnknownValue ) AS teamkey
       |FROM $viewFactTable AS fact
       |  LEFT OUTER JOIN $dimName AS dim ON fact.${mapDimColsToFactCols.head._2} <=> dim.${mapDimColsToFactCols.head._1}
       |                                AND fact.${effDateColAsOption.get} BETWEEN dim.StartDate AND dim.EndDate
       |""".stripMargin

    println(s"-- Setting fact key in dimension $dimName override using sql:\n$sqlToSetKey")

    val spark = SparkSession.builder().getOrCreate()
    val dfFactWithNewKey = spark.sql(sqlToSetKey)

    if (DwNFL.getIsDebug) {
      dfFactWithNewKey.printSchema()
      dfFactWithNewKey.show(10)

      dfFactWithNewKey
        .where("teamkey <> 0")
        .show(10)

    }

    dfFactWithNewKey
  }


}
