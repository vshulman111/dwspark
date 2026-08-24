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
import org.apache.spark.sql.DataFrame
import com.dbtimes.nfldw.DwNFL
import org.apache.spark.sql.SparkSession


/**
 *
 * @param dimName
 */
class DimTeamRole(dimName: String) extends Dim(dimName) {

  override def loadDim(stgSrcViewWithMonikerName: String): Option[DataFrame] = {

    val rows = List((1, DwNFL.teamRoleOffense), (2, DwNFL.teamRoleDefense), (3, DwNFL.teamRolePenalty), (4, DwNFL.teamRoleTimeout))
    // This is the syntax for a single column where a Tuple with a single column need to be used.
    // If , say, we want to generate the keys as surrogate keys
    //    val rows = List( Tuple1("Offense"), Tuple1("Defense"), Tuple1("Penalty"),  Tuple1("Timeout")	)

    val spark = SparkSession.builder().getOrCreate()
    val df = spark.createDataFrame(rows)
      .toDF( "TeamRoleKey", "TeamRole" )

    Some(df)
  }
}
