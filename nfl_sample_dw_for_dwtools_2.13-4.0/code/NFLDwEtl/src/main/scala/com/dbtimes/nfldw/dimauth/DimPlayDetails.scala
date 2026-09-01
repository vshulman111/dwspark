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

class DimPlayDetails(dimName: String) extends Dim(dimName) {

  override def loadDim(stgSrcViewWithMonikerName: String): Option[DataFrame] = {
    val sqlStgSource = if (stgSrcViewWithMonikerName == "PlayByPlay") {
      """|SELECT DISTINCT
         |	 IFNULL( CASE IsRush								          WHEN '0' THEN 'N' WHEN '1' THEN 'Y' ELSE CAST( IsRush AS STRING )								           END , '' )	AS IsRush							  ,
         |	 IFNULL( CASE IsPass								          WHEN '0' THEN 'N' WHEN '1' THEN 'Y' ELSE CAST( IsPass AS STRING )								           END , '' )	AS IsPass							  ,
         |	 IFNULL( CASE IsIncomplete					          WHEN '0' THEN 'N' WHEN '1' THEN 'Y' ELSE CAST( IsIncomplete AS STRING )					           END , '' )	AS IsIncomplete					,
         |	 IFNULL( CASE IsTouchdown					            WHEN '0' THEN 'N' WHEN '1' THEN 'Y' ELSE CAST( IsTouchdown AS STRING )					           END , '' ) AS IsTouchdown					,
         |	 IFNULL( CASE IsSack								          WHEN '0' THEN 'N' WHEN '1' THEN 'Y' ELSE CAST( IsSack AS STRING )								           END , '' ) AS IsSack							  ,
         |	 IFNULL( CASE IsChallenge					            WHEN '0' THEN 'N' WHEN '1' THEN 'Y' ELSE CAST( IsChallenge AS STRING )					           END , '' ) AS IsChallenge					,
         |	 IFNULL( CASE IsChallengeReversed	            WHEN '0' THEN 'N' WHEN '1' THEN 'Y' ELSE CAST( IsChallengeReversed AS STRING )	           END , '' ) AS IsChallengeReversed	,
         |	 IFNULL( CASE IsMeasurement				            WHEN '0' THEN 'N' WHEN '1' THEN 'Y' ELSE CAST( IsMeasurement AS STRING )				           END , '' ) AS IsMeasurement				,
         |	 IFNULL( CASE IsInterception				          WHEN '0' THEN 'N' WHEN '1' THEN 'Y' ELSE CAST( IsInterception AS STRING )				           END , '' ) AS IsInterception				,
         |	 IFNULL( CASE IsFumble							          WHEN '0' THEN 'N' WHEN '1' THEN 'Y' ELSE CAST( IsFumble AS STRING )							           END , '' ) AS IsFumble						  ,
         |	 IFNULL( CASE IsPenalty						            WHEN '0' THEN 'N' WHEN '1' THEN 'Y' ELSE CAST( IsPenalty AS STRING )						           END , '' ) AS IsPenalty						,
         |	 IFNULL( CASE IsTwoPointConversion	          WHEN '0' THEN 'N' WHEN '1' THEN 'Y' ELSE CAST( IsTwoPointConversion AS STRING )	           END , '' )	AS IsTwoPointConversion	,
         |	 IFNULL( CASE IsTwoPointConversionSuccessful	WHEN '0' THEN 'N' WHEN '1' THEN 'Y' ELSE CAST( IsTwoPointConversionSuccessful AS STRING )	 END , '' )	AS IsTwoPointConversionSuccessful	,
         |	 IFNULL( CASE IsPenaltyAccepted					      WHEN '0' THEN 'N' WHEN '1' THEN 'Y' ELSE CAST( IsPenaltyAccepted AS STRING )					     END , '' )	AS IsPenaltyAccepted		,
         |	 IFNULL( CASE IsNoPlay					              WHEN '0' THEN 'N' WHEN '1' THEN 'Y' ELSE CAST( IsNoPlay AS STRING )					               END , '' ) AS IsNoPlay
         |FROM PlayByPlay """.stripMargin
    }
    else
      throw new RuntimeException("""Dim Etl ERROR: Unknown stg source while loading dimension """)

    val spark = SparkSession.builder().getOrCreate()
    val dfStgSource = spark.sql(sqlStgSource)

    if (DwNFL.getIsDebug) {
      dfStgSource.printSchema()
      dfStgSource.show(10)
    }

    Some(dfStgSource)
  }

}
