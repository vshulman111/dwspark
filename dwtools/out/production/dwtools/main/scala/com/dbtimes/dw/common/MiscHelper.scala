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

import com.networknt.schema.dialect.Dialects
import com.networknt.schema.{InputFormat, SchemaRegistry, SpecificationVersion, SchemaRegistryConfig}

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}

import scala.jdk.CollectionConverters._
import scala.collection.mutable

object MiscHelper {

  /**
   *
   * @param appConfig
   * @param schemaBaseFileName
   * @param pathToVersionField - this path is specific to each configuration, e.g., for source loader it would be "sourceLoad.sourceLoadConfigVersion"
   *                           Given that I have schemas with version field name, this can be detected automatically, but i will leave this for later - TODO
   * @return
   */
  def validateConfigVersionAndConfigAgainstThatVersion(
      appConfig: Config,
      schemaBaseFileName: String,
      pathToVersionField: String ): Seq[String] = {
    val schemaVersionValidationFileName = schemaBaseFileName + "_version.json"

    // Verify appConfig
     val schemaRegistryConfig: SchemaRegistryConfig = SchemaRegistryConfig.builder()
      .errorMessageKeyword("customMessage")
      .build();

    val jsonToValidate = appConfig.root().render(ConfigRenderOptions.concise());
    val factory = SchemaRegistry.withDialect(Dialects.getDraft202012(), builder => builder.schemaRegistryConfig(schemaRegistryConfig));
    var errors: mutable.Seq[String] = mutable.Seq.empty[String]

    // Validate schema in three steps:
    // Step 1. Validate schema version of configuration file
    // Step 2. Load schema of correct version and validate configuration against that version
    // Step 3. Validate schema in the code for components not supported by generic  schema validation
    //    - unique names of actions

    for (step <- 1 to 2) {

      val configSchema = ConfigFactory.parseResources(
          if ( step == 1 ) {
            schemaVersionValidationFileName
          }
          else {
            // here we are in step 2. That means that version validation succeeded and we can safely extract the version from the configuration
            val configVersion = appConfig.getString(pathToVersionField) // previous validation insures that this path exists and has correct version number
            val schemaValidationFileName = schemaBaseFileName + s"""_v$configVersion.json"""
            schemaValidationFileName
          } ).resolve();
      val jsonSchema = configSchema.root().render(ConfigRenderOptions.concise());
      val schema = factory.getSchema(jsonSchema)

      // Validate the JSON data
      val validationMessages = schema.validate(jsonToValidate, InputFormat.JSON)
      // prepare return messages
      if (!validationMessages.isEmpty) {
        val listValidationMessages = validationMessages.asScala
        listValidationMessages.foreach(message => errors = errors :+ message.toString)
        return errors.toSeq // if step validation failed do not continue to the next step - return the errors
      }
    }


/*

    // Step 1. Validate schema version of configuration file
    val configSchemaVerion = ConfigFactory.parseResources(schemaVersionValidationFileName).resolve();
    val jsonSchemaVersion = configSchemaVerion.root().render(ConfigRenderOptions.concise());
    val schemaVersion = factory.getSchema(jsonSchemaVersion)

    // Validate the version
    val versionValidationMessages = schemaVersion.validate(jsonToValidate, InputFormat.JSON)
    // if version is not valid stop here and return the error message as we need version number for further validation
    if (!versionValidationMessages.isEmpty) {
      val listVersionValidationMessages = versionValidationMessages.asScala
      listVersionValidationMessages.foreach(message => errors = errors :+ message.toString)
      return errors.toSeq
    }

    // Step 2. Load schema of correct version and validate configuration against that version
    //    At this point we passed version validation. Load correct schema to validate against.
    val configVersion = appConfig.getString("sourceLoad.sourceLoadConfigVersion") // previous validation insures that this path exists and has correct version number
    val schemaValidationFileName = schemaBaseFileName + s"""_v$configVersion.json"""

    val configSchema = ConfigFactory.parseResources(schemaValidationFileName).resolve();
    val jsonSchema = configSchema.root().render(ConfigRenderOptions.concise());
    val schema = factory.getSchema(jsonSchema)

    // Validate the JSON data
    val validationMessages = schema.validate(jsonToValidate, InputFormat.JSON)
    // prepare return messages
    if (!validationMessages.isEmpty) {
      val listValidationMessages = validationMessages.asScala
      listValidationMessages.foreach(message => errors = errors :+ message.toString)
      return errors.toSeq
    }

 */

    errors.toSeq
  }

  def getDateFormatted(date: Date, formatPattern: String, offset: Int = 0): String = {
    val calendar: Calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.add(Calendar.DATE, offset)
    new SimpleDateFormat(formatPattern).format(calendar.getTime())
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

}
