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
package com.dbtimes.dw.common

import java.nio.file.FileSystems
import java.util.Calendar
import java.text.SimpleDateFormat

import org.apache.log4j.{FileAppender, LogManager, Logger, PatternLayout, SimpleLayout}
// import com.typesafe.config.Config

object LogFile {
  @transient lazy val logger = Logger.getLogger(getClass.getName)
  @transient lazy private val loggerSpark = Logger.getLogger("org.apache.spark")

  private[dw] def init(appName: String, appType: String, appSubTypeOptional: Option[String], logFileDirectory: String): Unit = {
    // load log file path from application configuration - the default config file name is Application.conf
    // and set the log file
    val fileSeparator = FileSystems.getDefault().getSeparator()
    if (fileSeparator == "\\") { // create file appender on Windows only for now as HDFS cannot write to Log
      // val logDir = appConfig.getString("dwEtlRunner.logFileDir")
      val layout = new PatternLayout(s"%d{ISO8601} $appType %5p: %m%n")
      val appender = new FileAppender(layout, FileHelper.makePath(logFileDirectory,
        appType
          + " _"
          + (if (appSubTypeOptional.isDefined) appSubTypeOptional.get + "_" else "") // appConfig.getString("dwEtlRunner.jobType")
          + appName
          + "_"
          + System.getProperty("user.name").replace("\\<>", "___") // sanitize file name. Replace all characters that are not valid in file name with underscores
          + "_"
          + new SimpleDateFormat("yyyyMMdd_hhmmss").format(Calendar.getInstance().getTime())
          + ".log"), false)
      logger.addAppender(appender)

      val appenderSpark = new FileAppender(layout, FileHelper.makePath(logFileDirectory,
        "spark_"
          + appType
          + " _"
          + (if (appSubTypeOptional.isDefined) appSubTypeOptional.get + "_" else "") // appConfig.getString("dwEtlRunner.jobType")
          + appName
          + "_"
          + System.getProperty("user.name").replace("\\<>", "___") // sanitize file name. Replace all characters that are not valid in file name with underscores
          + "_"
          + new SimpleDateFormat("yyyyMMdd_hhmmss").format(Calendar.getInstance().getTime())
          + ".log"), false)
      loggerSpark.addAppender(appenderSpark)

      //    logger.setLevel(Level.DEBUG.asInstanceOf[Nothing])
      // val log = LogManager.getRootLogger()
    }
  }

}
