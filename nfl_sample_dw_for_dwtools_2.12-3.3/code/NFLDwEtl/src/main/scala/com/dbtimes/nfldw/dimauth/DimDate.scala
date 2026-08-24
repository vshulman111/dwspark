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

import com.dbtimes.dw.etl.Dim
import com.dbtimes.nfldw.DwNFL

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

/**
 *
 * @param dimName
 */
class DimDate(dimName: String) extends Dim(dimName) {

/*  override def loadDim(stgSrcViewWithMonikerName: String): Option[DataFrame] = {
    val sqlStgSource
    =
      s"""| SELECT
          |   CAST( DATE_FORMAT($effDateColumnNameInDatesToProcess, 'yyyyMMdd') AS INT) AS DateKey,
          |   $effDateColumnNameInDatesToProcess AS Date,
          |   DATE_FORMAT($effDateColumnNameInDatesToProcess, 'yyyy-MM-dd') AS DateDesc,
          |   DATE_FORMAT($effDateColumnNameInDatesToProcess, 'MMM') AS MonthName,
          |   CAST( DATE_FORMAT($effDateColumnNameInDatesToProcess, 'yyyy') AS INT) AS Year,
          |   QUARTER($effDateColumnNameInDatesToProcess) AS Quarter,
          |   CONCAT( 'Q', CAST( QUARTER($effDateColumnNameInDatesToProcess) AS STRING ) ) AS QuarterDesc,
          |   CAST( IF( MONTH( $effDateColumnNameInDatesToProcess ) >= 8,
          |     CONCAT( DATE_FORMAT($effDateColumnNameInDatesToProcess, 'yyyy'), '-01-01' ),
          |     CONCAT( CAST( YEAR($effDateColumnNameInDatesToProcess) - 1 AS STRING), '-01-01' ) ) AS DATE ) AS SeasonYear,
          |   IF( MONTH( $effDateColumnNameInDatesToProcess ) >= 8,
          |     CONCAT( DATE_FORMAT($effDateColumnNameInDatesToProcess, 'yyyy'), '-', CAST( YEAR($effDateColumnNameInDatesToProcess) + 1 AS STRING ) ),
          |     CONCAT( CAST( YEAR($effDateColumnNameInDatesToProcess) - 1 AS STRING), '-', DATE_FORMAT($effDateColumnNameInDatesToProcess, 'yyyy') ) ) AS SeasonDescription
          | FROM $datesToProcessView """.stripMargin

    val spark = SparkSession.builder().getOrCreate()
    val dfStgSource = spark.sql(sqlStgSource)

    Some(dfStgSource)
  }*/

  override def loadDim(effDateYYYY_MM_DD: String, stgSrcViewWithMonikerName: String): Option[DataFrame] = {

    val sqlStgSource
    =
      s"""| SELECT
          |   CAST( DATE_FORMAT('$effDateYYYY_MM_DD', 'yyyyMMdd') AS INT) AS DateKey,
          |   CAST( '$effDateYYYY_MM_DD' AS DATE) AS Date,
          |   DATE_FORMAT('$effDateYYYY_MM_DD', 'yyyy-MM-dd') AS DateDesc,
          |   DATE_FORMAT('$effDateYYYY_MM_DD', 'MMM') AS MonthName,
          |   CAST( DATE_FORMAT('$effDateYYYY_MM_DD', 'yyyy') AS INT) AS Year,
          |   QUARTER('$effDateYYYY_MM_DD') AS Quarter,
          |   CONCAT( 'Q', CAST( QUARTER('$effDateYYYY_MM_DD') AS STRING ) ) AS QuarterDesc,
          |   CAST( IF( MONTH( '$effDateYYYY_MM_DD' ) >= 8,
          |     CONCAT( DATE_FORMAT('$effDateYYYY_MM_DD', 'yyyy'), '-01-01' ),
          |     CONCAT( CAST( YEAR('$effDateYYYY_MM_DD') - 1 AS STRING), '-01-01' ) ) AS DATE ) AS SeasonYear,
          |   IF( MONTH( '$effDateYYYY_MM_DD' ) >= 8,
          |     CONCAT( DATE_FORMAT('$effDateYYYY_MM_DD', 'yyyy'), '-', CAST( YEAR('$effDateYYYY_MM_DD') + 1 AS STRING ) ),
          |     CONCAT( CAST( YEAR('$effDateYYYY_MM_DD') - 1 AS STRING), '-', DATE_FORMAT('$effDateYYYY_MM_DD', 'yyyy') ) ) AS SeasonDescription
           """.stripMargin
    val spark = SparkSession.builder().getOrCreate()
    val dfStgSource = spark.sql(sqlStgSource)

    if (DwNFL.getIsDebug) {
      dfStgSource.show(10);
      dfStgSource.printSchema()
    }

    Some(dfStgSource)
  }

/*  override def getCustomDatesToProcess(dfDatesToProcessBasedOnConfig: DataFrame): Option[DataFrame] = {
    // For DimTeans we need one date per  season
    dfDatesToProcessBasedOnConfig.createOrReplaceTempView("DatesToProcessBasedOnConfig")

    if (DwNFL.getIsDebug) {
      val rows = dfDatesToProcessBasedOnConfig.count()
      dfDatesToProcessBasedOnConfig.show(50);
      dfDatesToProcessBasedOnConfig.printSchema()
    }

    val sqlDimDatesToProcess
    =
      s"""|SELECT * FROM DatesToProcessBasedOnConfig LIMIT 200
					|  """.stripMargin
    val dfDimDatesToProcess = spark.sql(sqlDimDatesToProcess)

    if (DwNFL.getIsDebug) {
      val rows = dfDimDatesToProcess.count()
      dfDimDatesToProcess.show(5);
      dfDimDatesToProcess.printSchema()
    }

    Some(dfDimDatesToProcess)
  }*/
}
