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
package com.dbtimes.nfldw

import java.util.Date

import org.apache.spark.storage.StorageLevel
import java.sql.{Date => SqlDate}
import java.util.Calendar
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

import com.typesafe.config.{Config, ConfigFactory}
import scala.jdk.CollectionConverters._

import com.dbtimes.dw.etl.ModelObject
import com.dbtimes.dw.common.Utils
import com.typesafe.scalalogging.LazyLogging

/**
 * The purpose of this object it to initialize shared variables and DataFrames,
 * register UDFs, etc.
 *
 */
object DwNFL  extends LazyLogging {
  val teamRoleOffense = "Offense"
  val teamRoleDefense = "Defense"
  val teamRolePenalty = "Penalty"
  val teamRoleTimeout = "Timeout"

  private var stgSourcesMonikerDescriptionTransactionName: List[(String, String, String)] = List.empty
  private var isDebug: Boolean = false
  val etlLogger = logger

  def main(args: Array[String]): Unit = {

    val configFileName = if (args.length >= 1) args(0) else throw new RuntimeException("""ERROR: Need to pass a parameter - configuration file name """)
    // val appConfig = ConfigFactory.load(configFileName)
    val appConfig = Utils.loadConfigAndApplyOverrides( configFileName, args.drop(1) )

    // validate configuration
    ModelObject.validateConfig(appConfig: Config)

    val sparkSessionBuilder = DwNFL.configureSparkSession(appConfig)
    val spark = sparkSessionBuilder.getOrCreate()

    // Do initialization specific to a given DW
    DwNFL.init(appConfig)

    // ETL process to build or update dimensions and fact tables
    ModelObject.runEtl(appConfig )

    spark.stop()
  }

  def getIsDebug: Boolean = isDebug
  def getStgSourcesMonikerDescriptionTransactionName: List[(String, String, String)] = stgSourcesMonikerDescriptionTransactionName

  private def configureSparkSession(appConfig: Config): SparkSession.Builder = {
    val sparkConfig = Utils.getConfigObjectFields( if ( appConfig.hasPath("sparkParams" ) ) Some( appConfig.getConfig("sparkParams") ) else None )
    val conf = new SparkConf( )
      .setAll(sparkConfig)

    SparkSession.builder().config(conf)
  }

  private def init(appConfig: Config): Unit = {
    val spark = SparkSession.builder().getOrCreate() // this gets previously created session
    spark.udf.register("udfSeasonYear", DwNFL.getSeasonYear)

    val stgSources = appConfig.getConfigList("dwEtl.stgSources").asScala.toList
    stgSourcesMonikerDescriptionTransactionName = stgSources map {
      case source: Config => {
        (source.getString("moniker"),
          if (source.hasPath("description")) source.getString("description") else "",
          if (source.hasPath("applicationSpecific.transactionName")) source.getString("applicationSpecific.transactionName") else "")
      }
    }

    isDebug = if (appConfig.hasPath("isDebug")) {
      appConfig.getBoolean("isDebug")
    } else {
      false // default - no debugging
    }
  }

  private val getSeasonYear = (effDate: SqlDate) => {
    val cal = Calendar.getInstance()
    cal.setTime(effDate)
    val year = cal.get(Calendar.YEAR)
    val month = cal.get(Calendar.MONTH)
    if (month >= 8) year else year - 1
  }

}
