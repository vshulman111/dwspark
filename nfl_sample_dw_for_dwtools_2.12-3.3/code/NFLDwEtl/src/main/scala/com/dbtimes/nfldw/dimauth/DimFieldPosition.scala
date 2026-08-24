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

class DimFieldPosition(dimName: String) extends Dim(dimName) {

  override def loadDim(stgSrcViewWithMonikerName: String): Option[DataFrame] = {
    val sqlStgSource = if (stgSrcViewWithMonikerName == "PlayByPlay") {
      s"""| SELECT DISTINCT
         |   IFNULL( CAST( Down				        AS STRING ), '' )			 AS Down,
         |   IFNULL( CAST( YardLine			      AS STRING ), '' )		   AS YardLine,
         |   IFNULL( CAST( RushDirection		  AS STRING ), '' )	     AS RushDirection,
         |   IFNULL( CAST( YardLineFixed		  AS STRING ), '' )	     AS YardLineFixed,
         |   IFNULL( CAST( YardLineDirection	AS STRING ), '' )      AS YardLineDirection
         | FROM $stgSrcViewWithMonikerName """.stripMargin
    }
    else
      throw new RuntimeException(s"""Dim Etl ERROR: Unknown stg source while loading dimension $dimName""")

    val spark = SparkSession.builder().getOrCreate()
    val dfStgSource = spark.sql(sqlStgSource)

    dfStgSource.printSchema()

    if (DwNFL.getIsDebug) {
      dfStgSource.show(10)
    }

    Some(dfStgSource)
  }

}
