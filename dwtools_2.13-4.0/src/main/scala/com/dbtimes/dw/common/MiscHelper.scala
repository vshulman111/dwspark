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
import scala.reflect.runtime.universe._

private [dw] object MiscHelper {

/**
   *
   * @param appConfig
   * @param schemaBaseFileName
   * @return - tuple (errors,warnings)
   */

  private[dw] def validateConfig(
      appConfig: Config,
      schemaBaseFileName: String ): ( Seq[String], Seq[String] ) = {

    // Verify appConfig
     val schemaRegistryConfig: SchemaRegistryConfig = SchemaRegistryConfig.builder()
      .errorMessageKeyword("customMessage")
      .build();

    val jsonToValidate = appConfig.root().render(ConfigRenderOptions.concise());
    val factory = SchemaRegistry.withDialect(Dialects.getDraft202012(), builder => builder.schemaRegistryConfig(schemaRegistryConfig));
    var errorsAndWarnings: mutable.Seq[String] = mutable.Seq.empty[String]

    // Validate schema in two steps:
    // Step 1. Load schema of correct version and validate configuration against that version
    // Step 2. Validate schema in the code for components not supported by generic  schema validation
    //    - unique names of actions

    val schemaValidationFileName = schemaBaseFileName + ".json"
    val configSchema = ConfigFactory.parseResources( schemaValidationFileName ).resolve();
    val jsonSchema = configSchema.root().render(ConfigRenderOptions.concise());
    val schema = factory.getSchema(jsonSchema)

    // Validate the JSON data
    val validationMessages = schema.validate(jsonToValidate, InputFormat.JSON)
    // prepare return messages
    if (!validationMessages.isEmpty) {
      val listValidationMessages = validationMessages.asScala
      listValidationMessages.foreach(message => errorsAndWarnings = errorsAndWarnings :+ message.toString)
    }

    // Split into errors and warnings
    errorsAndWarnings.toSeq.partition(!_.contains("[WARN]"))
  }

  private[dw] def getDateFormatted(date: Date, formatPattern: String, offset: Int = 0): String = {
    val calendar: Calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.add(Calendar.DATE, offset)
    new SimpleDateFormat(formatPattern).format(calendar.getTime())
  }

  private[dw] def getInvokerForDynamicMethodInvokation( packageName: String, moduleName: String, methodName: String ): MethodMirror = {

    // 1. Obtain a runtime mirror
    val mirror = runtimeMirror(getClass.getClassLoader)

    // 2. Get the module (object) symbol
    val moduleSymbol = mirror.staticModule( if ( packageName == "") moduleName else packageName + "." + moduleName )

    // 3. Get the module mirror (instance mirror for the object)
    val moduleMirror = mirror.reflectModule(moduleSymbol)
    val instance = moduleMirror.instance                  // The singleton instance of the object

    // 4. Get the method symbol
    val methodSymbol = moduleSymbol.info.decl(TermName(methodName)).asMethod

    // 5. Get the instance mirror for the object's instance
    val invoker = mirror.reflect(instance).reflectMethod(methodSymbol)

    invoker
  }

}
