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
package com.dbtimes.nflsourceloader

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.typesafe.config.{ConfigFactory, Config}
import com.dbtimes.dw.sourceloader.DataSourceLoader
import com.dbtimes.dw.common.Utils
// import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.LazyLogging

object SrcLoaderNFL extends LazyLogging {
  val loaderLogger = logger

  def main(args: Array[String]): Unit = {

    val configFileName = if (args.length == 1) args(0) else throw new RuntimeException("""ERROR: Need to pass a parameter - configuration file name """)
    val appConfig = ConfigFactory.load(configFileName)

    // validate configuration
    DataSourceLoader.validateConfig(appConfig: Config)

    val sparkSessionBuilder = SrcLoaderNFL.configureSparkSession(appConfig)
    val spark = sparkSessionBuilder.getOrCreate()

    DataSourceLoader.loadData(appConfig )
    
	if( Utils.isSparkRunningLocally() )
      spark.stop()
  }

  def configureSparkSession(appConfig: Config): SparkSession.Builder = {
    val sparkConfig = Utils.getConfigObjectFields( if ( appConfig.hasPath("sparkParams" ) ) Some( appConfig.getConfig("sparkParams") ) else None )
    val conf = new SparkConf( )
      .setAll(sparkConfig)
    SparkSession.builder().config(conf)
  }

  def postProcessPlayers( appSpecificAsOption: Option[Config], dfSource: DataFrame ): DataFrame = {
    val spark = SparkSession.builder().getOrCreate() // this gets previously created session
    val appSpecific = appSpecificAsOption.get
    val field1 = appSpecific.getString("field1")  // result - "value1"
    val field2 = appSpecific.getString("field2")  // result - "value2"
    val viewSource = "Players" + java.util.UUID.randomUUID.toString.replace("-", "_" )
    val sql = s"""
            |SELECT *
            |FROM $viewSource
            |WHERE draft_team IN (SELECT DISTINCT OffenceTeam FROM PlayByPlay ) """.stripMargin
    val df = spark.sql(sql)
    df
  }

}
