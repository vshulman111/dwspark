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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import com.dbtimes.nfldw.DwNFL

/**
 *
 * @param factName
 */
class BridgeTeamsPlays(factName: String) extends FactsNFL(factName) {

  override def loadFact(effDateYYYY_MM_DD: String, stgSrcViewWithMonikerName: String): Option[DataFrame] = {

    val dfFact = if (stgSrcViewWithMonikerName == "PlayByPlay") {
      val sqlStgSource =
        s"""
           |WITH PlaysWithTeamAndRoles
           |AS
           |		(
           |			SELECT	*,
           |					OffenseTeam		AS TeamAbbrev,
           |					'${DwNFL.teamRoleOffense}'	AS TeamRole
           |			FROM $stgSrcViewWithMonikerName
           |			WHERE	SeasonYear = udfSeasonYear( CAST( '$effDateYYYY_MM_DD' AS DATE) )
           |					AND OffenseTeam IS NOT NULL
           |					AND PlayType != 'TIMEOUT'		-- All teams are defense for timeout, so looks like need to exclude timeout, since the team is determined from Description
           |			UNION ALL
           |			SELECT	*,
           |					DefenseTeam		AS TeamAbbrev,
           |					'${DwNFL.teamRoleDefense}'	AS TeamRole
           |			FROM $stgSrcViewWithMonikerName
           |			WHERE	SeasonYear = udfSeasonYear( CAST( '$effDateYYYY_MM_DD' AS DATE) )
           |					AND DefenseTeam IS NOT NULL
           |					AND PlayType != 'TIMEOUT'		-- All teams are defense for timeout, so looks like need to exclude timeout, since the team is determined from Description
           |			UNION ALL
           |			SELECT	*,
           |					PenaltyTeam		         AS TeamAbbrev,
           |					'${DwNFL.teamRolePenalty}'	AS TeamRole
           |			FROM $stgSrcViewWithMonikerName
           |			WHERE	SeasonYear = udfSeasonYear( CAST( '$effDateYYYY_MM_DD' AS DATE) )
           |					AND PenaltyTeam IS NOT NULL
           |			UNION ALL
           |			SELECT	*,
           |					RTRIM( SUBSTRING( Description, INSTR( 'BY ', Description ) + 3, 3 ) )	AS TeamAbbrev,
           |					'${DwNFL.teamRoleTimeout}'	AS TeamRole
           |			FROM $stgSrcViewWithMonikerName
           |			WHERE	SeasonYear = udfSeasonYear( CAST( '$effDateYYYY_MM_DD' AS DATE) )
           |					AND PlayType = 'TIMEOUT'
           |         AND INSTR( 'BY ', Description ) > 0   -- this will exclude timeouts not called by a team, e.g., this one TIMEOUT AT 07:46. INJURY TO OFFICIAL. in game 2015110104
           |		)
           |SELECT
					|		-- SeasonYear																																	    AS SeasonYear,  -- This is partitioning column. Create Bridge without it
					|		"$stgSrcViewWithMonikerName"																																AS DataSourceMoniker,
					|	  CAST( GameId AS STRING )																										    AS GameId,
					|		CAST( GameDate AS DATE )																										    AS PlayDate,
           |		TeamAbbrev					                                                            AS TeamAbbrevName	,
           |		TeamRole					                                                              AS TeamRole		,
					|   IFNULL( Formation				, '' )																									AS Formation		,
					|   IFNULL( PlayType			, '' )																										AS PlayType		,
					|   IFNULL( PassType		, '' )	 																										AS PassType		,
					|   IFNULL( PenaltyType		, '' )																										AS PenaltyType	,
					|   IFNULL( CAST( Down				      AS STRING ), '' )																											AS Down				,
					|   IFNULL( CAST( YardLine			    AS STRING ), '' )																										AS YardLine		,
					|   IFNULL( CAST( RushDirection		  AS STRING ), '' )																									AS RushDirection		,
					|   IFNULL( CAST( YardLineFixed		  AS STRING ), '' )																									AS YardLineFixed		,
					|   IFNULL( CAST( YardLineDirection	AS STRING ), '' )																								AS YardLineDirection,
					|   IFNULL( CAST( Quarter           AS STRING ), '' )																														AS Quarter,
					|   IFNULL( CAST( Minute            AS STRING ), '' ) 																														AS Minute,
					|   IFNULL( CAST( Second            AS STRING ), '' )																														AS Second
					|FROM PlaysWithTeamAndRoles
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
