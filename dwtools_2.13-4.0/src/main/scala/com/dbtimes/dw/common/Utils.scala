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

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import java.util.{Calendar, Date}

/***
 * Exposes some methods
 */

object Utils {

  def getDateFormatted(date: Date, formatPattern: String, offset: Int = 0): String = {
    MiscHelper.getDateFormatted(date, formatPattern, offset)
  }

  def getConfigObjectFields(
      configObjectAsOption: Option[Config] ): Map[String,String] = {

    new ConfigDwJobsCommon().getConfigObjectFieldsAndValues(configObjectAsOption)
  }

  def loadConfigAndApplyOverrides(resourceFileName: String, configOverrides: Array[String] ): Config = {
    val initialConfig = ConfigFactory.load(resourceFileName)

    val appConfig = configOverrides.foldLeft(initialConfig) {
      case (currConfig, configOverride) => ConfigFactory.parseString(configOverride)
        .withFallback(currConfig) // merge with main configuration
        .resolve() // process variable substitutions (${...}).
    }

    appConfig
  }

  def createFileNameWithCurrentTimestamp(filePathDest: String, sourceFilePathOrUrl: String, defaultFileName: String): String = {
    FileHelper.createFileNameWithCurrentTimestamp(filePathDest, sourceFilePathOrUrl, defaultFileName)
  }

}
