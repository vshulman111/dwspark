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

class DimGame(dimName: String) extends Dim(dimName) {

  override def loadDim(stgSrcViewWithMonikerName: String): Option[DataFrame] = {
    val dfStgSourceAsOption = if (stgSrcViewWithMonikerName == "PlayByPlay") {
      val sqlPlayByPlay =
        """
          | SELECT
          |  CAST( GameId AS STRING ) AS GameId,
          |  MAX( CONCAT( OffenseTeam,'/',DefenseTeam ) ) AS GameTeams
          | FROM PlayByPlay
          | GROUP BY GameId """.stripMargin

      DwNFL.etlLogger.info( s"sqlPlayByPlay:\n $sqlPlayByPlay" )

      val spark = SparkSession.builder().getOrCreate()
      Some(spark.sql(sqlPlayByPlay))
    }
    else if (stgSrcViewWithMonikerName == "Teams") {
      None // Do not need to process this source
    }
    else
      throw new RuntimeException("""Dim Etl ERROR: Unknown stg source while loading dimension """)

    dfStgSourceAsOption
  }

}
