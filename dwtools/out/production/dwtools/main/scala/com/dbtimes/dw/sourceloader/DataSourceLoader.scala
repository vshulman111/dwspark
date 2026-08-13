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
package com.dbtimes.dw.sourceloader

import com.dbtimes.dw.common._
import com.typesafe.config.{ConfigRenderOptions, ConfigValue}
import com.dbtimes.dw.common.DataFrameHelper.{DataFrameImplicits, concatColumns}
import com.dbtimes.dw.common.MiscHelper.getDateFormatted
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigValueType}

import scala.jdk.CollectionConverters._
import scala.collection.mutable
import com.networknt.schema.{InputFormat, Schema, SchemaLocation, SchemaRegistry, SpecificationVersion}

import LogFile.{logger => loaderLog} // rename to loaderLog

object DataSourceLoader {

  def validateConfig(appConfig: Config): Seq[String] = {

    val schemaBaseFileName = "sourceloader-config-schema"
    val pathToVersionField = "sourceLoad.sourceLoadConfigVersion"
    var errors: mutable.Seq[String] = mutable.Seq.empty[String]

    // Validate schema in three steps:
    // Step 1. Validate schema version of configuration file
    // Step 2. Load schema of correct version and validate configuration against that version
    // Step 3. Validate schema in the code for components not supported by generic  schema validation
    //    - unique names of actions

    // Do Steps 1 and 2
    errors ++= MiscHelper.validateConfigVersionAndConfigAgainstThatVersion( appConfig, schemaBaseFileName, pathToVersionField )

    // Step 3. Validate schema in the code for components not supported by generic  schema validation
    //          Step 3 depends on schema being validated in steps 1 and 2 because it assumes that path "sourceLoad.loadActions" exists and each action has a name
    if ( errors.isEmpty ) {
      // check for unique names of actions
      val loadActionConfigs: List[ConfigValue] = appConfig.getList("sourceLoad.loadActions").asScala.toList
      val actionNames: List[String] = loadActionConfigs map { case loadActionConfig: ConfigValue => {
        val action = new LoadAction(loadActionConfig);
        action.getName;
      }
      }
      val duplicateNames = actionNames.groupBy(identity)
        .collect { case (x, ys) if ys.size > 1 => x }
      if (!duplicateNames.isEmpty) {
        errors = errors :+ s"""The following action names are duplicates: ${duplicateNames.mkString("'", "', '", "'")}. All action names must be unique."""
      }

      // do custom validation of each action
      loadActionConfigs foreach { case loadActionConfig: ConfigValue => {
        val action = new LoadAction(loadActionConfig);
        errors ++= action.validate
      }
      }
    }

    errors.toSeq
  }

  def loadData(appConfig: Config): Unit = {

    // validate configuration just in case it was not validated
    val errors: Seq[String] = DataSourceLoader.validateConfig(appConfig: Config)
    if ( !errors.isEmpty ) {
      var errorMessage: String = ""
      errors.foreach(error => errorMessage = errorMessage + s"* ${error}\n")
      throw new RuntimeException("Loader Configuration ERROR(s)\n" + errorMessage)
    }

    val spark = SparkSession.builder().getOrCreate() // this gets previously created session
    val appName = try {
      spark.sparkContext.appName
    }
    catch {
      case e: Exception => ""
    }

    // Create log file if logFileDir is defined
    if (appConfig.hasPath("sourceLoad.logFileDir")) {
      LogFile.init(appName, "SOURCE_LOADER", None, appConfig.getString("sourceLoad.logFileDir"))
    }

    loaderLog.info("---- Starting application " + appName)

    // loop through all configurations and perform the loads
    val loadActionConfigs: List[ConfigValue] = appConfig.getList("sourceLoad.loadActions").asScala.toList
    loadActionConfigs.foreach(processLoadAction)

    loaderLog.info("---- Completed application " + appName)
  }

  private def processLoadAction(loadActionConfig: ConfigValue): Unit = {
    val action = new LoadAction(loadActionConfig)
    loaderLog.info("---- Processing load action: " + action.getName)
    if (!action.getIsActive) {
      loaderLog.info("Skipping processing - the action is inactive")
    }
    else { // active action - process it
      ActionProcessorFactory.getLoadActionProcessor(action).process
    }
  }

}
