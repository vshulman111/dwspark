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
class DimDataSource(dimName: String) extends Dim(dimName) {

  override def loadDim(stgSrcViewWithMonikerName: String): Option[DataFrame] = {

    val spark = SparkSession.builder().getOrCreate()
    val dfSources = spark.createDataFrame(DwNFL.getStgSourcesMonikerDescriptionTransactionName)
      .toDF( "DataSourceMoniker", "DataSourceDescription", "TransactionName" )

    dfSources.printSchema()

    Some(dfSources)
  }
}
