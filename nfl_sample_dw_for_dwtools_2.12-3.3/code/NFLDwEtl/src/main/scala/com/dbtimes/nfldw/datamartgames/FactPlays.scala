/*
--  Copyright (c) 2007 - 2021 by Victor Shulman.
--  Written by Victor Shulman.  Not derived from licensed software.
--
--  Permission is granted to anyone to use this software for any
--  purpose on any computer system, and to redistribute it in any way,
--  subject to the following restrictions:
--
--  1. The author is not responsible for the consequences of use of
--    this software, no matter how awful, even if they arise
--    from defects in it.
--
--  2. The origin of this software must not be misrepresented, either
--    by explicit claim or by omission.
--
--  3. This notice must not be removed or altered.
*/
package com.dbtimes.nfldw.datamartgames

import com.dbtimes.nfldw.DwNFL
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

/**
 *
 * @param factName
 */
class FactPlays(factName: String) extends FactsNFL(factName) {

  override def loadFact(stgSrcViewWithMonikerName: String): Option[DataFrame] = {

    if ( !isStgSourceChangedSinceLastLoad(stgSrcViewWithMonikerName) ) {
      println(s"-- Skipping loading fact $factName from source $stgSrcViewWithMonikerName as is has not changed since last load")
      None
    }
    else {
      val dfFact = if (stgSrcViewWithMonikerName == "PlayByPlay") {
        val sqlStgSource =
          s"""
             |SELECT
             |  src.SeasonYear                                                                      AS SeasonYear,
             |  "$stgSrcViewWithMonikerName"                                                                    AS DataSourceMoniker,
             |  CAST( src.GameId AS STRING )                                                        AS GameId,
             |  CAST( src.GameDate AS DATE )                                                        AS PlayDate,
             |  CASE WHEN src.OffenseTeam IS NOT NULL AND NOT( src.PlayType <=> 'TIMEOUT' )
             |       THEN src.OffenseTeam
             |       ELSE ''
             |       END                                                                            AS OffenseTeamAbbrevName          ,
             |  CASE WHEN src.DefenseTeam IS NOT NULL AND NOT( src.PlayType <=> 'TIMEOUT' )
             |        THEN src.DefenseTeam
             |       ELSE ''
             |       END                                                                            AS DefenseTeamAbbrevName          ,
             |  IFNULL( src.PenaltyTeam, '' )                                                        AS PenaltyTeamAbbrevName          ,
             |  CASE WHEN src.PlayType = 'TIMEOUT' AND INSTR( 'BY ', src.Description ) > 0
             |       THEN RTRIM( SUBSTRING( src.Description, INSTR( 'BY ', src.Description ) + 3, 3 ) )
             |       ELSE ''
             |       END                                                                            AS TimeoutTeamAbbrevName          ,
             |  IFNULL( src.Formation        , '' )                                                 AS Formation    ,
             |  IFNULL( src.PlayType      , '' )                                                    AS PlayType    ,
             |  IFNULL( src.PassType    , '' )                                                      AS PassType    ,
             |  IFNULL( src.PenaltyType    , '' )                                                   AS PenaltyType  ,
             |  IFNULL( CAST( src.Down               AS STRING ), '' )                              AS Down        ,
             |  IFNULL( CAST( src.YardLine           AS STRING ), '' )                              AS YardLine    ,
             |  IFNULL( CAST( src.RushDirection      AS STRING ), '' )                              AS RushDirection    ,
             |  IFNULL( CAST( src.YardLineFixed      AS STRING ), '' )                              AS YardLineFixed    ,
             |  IFNULL( CAST( src.YardLineDirection  AS STRING ), '' )                              AS YardLineDirection,
             |  IFNULL(  CAST( src.Quarter AS STRING), '' )                                         AS Quarter,
             |  IFNULL(  CAST( src.Minute  AS STRING), '' )                                         AS Minute,
             |  IFNULL(  CAST( src.Second  AS STRING), '' )                                         AS Second,
             |  IFNULL( CASE src.IsRush                          WHEN '0' THEN 'N' WHEN '1' THEN 'Y' ELSE 'N' END , 'N' ) AS IsRush                ,
             |  IFNULL( CASE src.IsPass                          WHEN '0' THEN 'N' WHEN '1' THEN 'Y' ELSE 'N' END , 'N' ) AS IsPass                ,
             |  IFNULL( CASE src.IsIncomplete                    WHEN '0' THEN 'N' WHEN '1' THEN 'Y' ELSE 'N' END , 'N' ) AS IsIncomplete          ,
             |  IFNULL( CASE src.IsTouchdown                     WHEN '0' THEN 'N' WHEN '1' THEN 'Y' ELSE 'N' END , 'N' ) AS IsTouchdown          ,
             |  IFNULL( CASE src.IsSack                          WHEN '0' THEN 'N' WHEN '1' THEN 'Y' ELSE 'N' END , 'N' ) AS IsSack                ,
             |  IFNULL( CASE src.IsChallenge                     WHEN '0' THEN 'N' WHEN '1' THEN 'Y' ELSE 'N' END , 'N' ) AS IsChallenge          ,
             |  IFNULL( CASE src.IsChallengeReversed             WHEN '0' THEN 'N' WHEN '1' THEN 'Y' ELSE 'N' END , 'N' ) AS IsChallengeReversed  ,
             |  IFNULL( CASE src.IsMeasurement                   WHEN '0' THEN 'N' WHEN '1' THEN 'Y' ELSE 'N' END , 'N' ) AS IsMeasurement        ,
             |  IFNULL( CASE src.IsInterception                  WHEN '0' THEN 'N' WHEN '1' THEN 'Y' ELSE 'N' END , 'N' ) AS IsInterception        ,
             |  IFNULL( CASE src.IsFumble                        WHEN '0' THEN 'N' WHEN '1' THEN 'Y' ELSE 'N' END , 'N' ) AS IsFumble              ,
             |  IFNULL( CASE src.IsPenalty                       WHEN '0' THEN 'N' WHEN '1' THEN 'Y' ELSE 'N' END , 'N' ) AS IsPenalty            ,
             |  IFNULL( CASE src.IsTwoPointConversion            WHEN '0' THEN 'N' WHEN '1' THEN 'Y' ELSE 'N' END , 'N' ) AS IsTwoPointConversion  ,
             |  IFNULL( CASE src.IsTwoPointConversionSuccessful  WHEN '0' THEN 'N' WHEN '1' THEN 'Y' ELSE 'N' END , 'N' ) AS IsTwoPointConversionSuccessful  ,
             |  IFNULL( CASE src.IsPenaltyAccepted               WHEN '0' THEN 'N' WHEN '1' THEN 'Y' ELSE 'N' END , 'N' ) AS IsPenaltyAccepted    ,
             |  IFNULL( CASE src.IsNoPlay                        WHEN '0' THEN 'N' WHEN '1' THEN 'Y' ELSE 'N' END , 'N' ) AS IsNoPlay              ,
             |  CAST( CASE
             |       WHEN src.IsTouchdown = 1
             |        THEN 6
             |        WHEN src.IsTwoPointConversionSuccessful = 1
             |        THEN 2
             |        WHEN src.PlayType = '$playExtraPoint'
             |           AND src.Description LIKE '%$playExtraPointIsGood%'
             |        THEN 1
             |        WHEN src.PlayType = '$playFieldGoal'
             |           AND src.Description LIKE '%$playFieldGoalIsGood%'
             |        THEN 3
             |       ELSE 0
             |        END AS DOUBLE )                                                                AS Score          ,
             |   CAST( src.Yards    AS DOUBLE )                                                      AS Yards          ,
             |   CAST( src.ToGo    AS DOUBLE )                                                       AS ToGo           ,
             |   CAST( src.PenaltyYards      AS DOUBLE )                                             AS PenaltyYards
             |FROM $stgSrcViewWithMonikerName AS src
             |   INNER JOIN $datesToProcessView AS dates   ON  src.SeasonYear = udfSeasonYear( dates.$effDateColumnNameInDatesToProcess )
             |
             | """.stripMargin

        println(s"-- Loading fact $factName from source $stgSrcViewWithMonikerName using sql:\n" + sqlStgSource)

        val spark = SparkSession.builder().getOrCreate()
        val dfStgSource = spark.sql(sqlStgSource)

        dfStgSource.dropDuplicates()
      }
      else {
        throw new RuntimeException(s"""Etl ERROR: Unknown stg source while loading fact $factName""")
      }

      if (DwNFL.getIsDebug) {
        dfFact.printSchema()
        dfFact.show(40)
      }

      Some(dfFact)
    }
  }
}
