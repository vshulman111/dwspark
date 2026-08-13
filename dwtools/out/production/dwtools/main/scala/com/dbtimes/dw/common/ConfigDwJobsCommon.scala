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

import java.text.SimpleDateFormat
import java.text.DateFormat
import java.util.Date

import com.typesafe.config.Config
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{StructField, StructType}

import scala.jdk.CollectionConverters._ // is needed for .asScala method when getting a list
import scala.collection.immutable.{ListMap}
import scala.reflect.runtime.universe._
import org.apache.spark.sql.types._


private[dw] class ConfigDwJobsCommon {

  /**
   *
   * @param schemaJsonRoot
   * @param schemaJsonPath
   * @return - returned schema is for spark consumption with all custom fields, e.g., "isUniqueKey", filtered out
   *         if schema is absent in configuration file - the empty schema is returned
   */

  protected def getSchema(schemaJsonRoot: Config, schemaJsonPath: String): StructType = {
    if ( schemaJsonRoot.hasPath(schemaJsonPath) ) {
      val sourceSchema = schemaJsonRoot.getConfigList(schemaJsonPath).asScala.toList
      val columns = sourceSchema map {
        case column: Config => {
          val columnName = column.getString("colName")
          val columnType = column.getString("colType")
          // By default, the column is nullable
          val columnIsNullable = if (!column.hasPath("isNullable")) true else column.getBoolean("isNullable")
          StructField(columnName, CatalystSqlParser.parseDataType(columnType), columnIsNullable)
        }
      }
      StructType(columns)
    }
    else {
      StructType(Seq())
    }
  }

  protected def getSchemaColNames(schemaJsonRoot: Config, schemaJsonPath: String): List[String] = {
    if ( schemaJsonRoot.hasPath(schemaJsonPath) ) {
      val sourceSchema = schemaJsonRoot.getConfigList(schemaJsonPath).asScala.toList
      sourceSchema map { case column: Config => column.getString("colName") }
    }
    else {
      List.empty[String]
    }
  }

  protected def getSchemaColNamesAndPropertyValues(
      schemaJsonRoot: Config,
      schemaJsonPath: String,
      propertyName: String): ListMap[String, String] = {

    if ( schemaJsonRoot.hasPath(schemaJsonPath) ) {
      val sourceSchema = schemaJsonRoot.getConfigList(schemaJsonPath).asScala.toList
      val columns = sourceSchema filter {
        case column: Config => {
          if (column.hasPath(propertyName))
            true
          else
            false
        }
      }
      val colNameAndProperty = columns map {
        case column: Config => (column.getString("colName"), column.getString(propertyName))
      } // return column with given property set
      ListMap(colNameAndProperty: _*)
    }
    else {
      ListMap.empty[String, String]
    }
  }

  /**
   * Set nullable property of column.
   * "colName" is the expected tag name of the column name in schema json definition
   *
   * @param flagName           is the name of the Boolean flag used to filter columns
   * @param flagValueToInclude is the flag value that will result in inclusion of the column in the result
   * @param flagValueDefault   is the default value to be used if the flag does not appear in the column definition.
   *                           If None - there is no default
   */
  protected def getSchemaColNamesWithFlag(
      schemaJsonRoot: Config,
      schemaJsonPath: String,
      flagName: String,
      flagValueToInclude: Boolean,
      flagValueDefault: Option[Boolean]): List[String] = {

    if ( schemaJsonRoot.hasPath(schemaJsonPath) ) {
      val sourceSchema = schemaJsonRoot.getConfigList(schemaJsonPath).asScala.toList
      val columns = sourceSchema filter {
        case column: Config => {
          if (column.hasPath(flagName)) {
            if (column.getBoolean(flagName) == flagValueToInclude)
              true
            else
              false
          }
          else if (flagValueDefault.isDefined && flagValueToInclude == flagValueDefault.get)
            true
          else
            false
        }
      }
      columns map { case column: Config => column.getString("colName") } // return column with given property set
    }
    else {
      List.empty[String]
    }
  }

  protected def getSchemaDistinctPropertyValuesWithFlag(
      schemaJsonRoot: Config,
      schemaJsonPath: String,
      propertyName: String,
      flagName: String,
      flagValueToInclude: Boolean,
      flagValueDefault: Option[Boolean]): List[String] = {

    if ( schemaJsonRoot.hasPath(schemaJsonPath) ) {

      // These are the columns with flag set to the value to include
      val columnsWitFlag = getSchemaColNamesWithFlag(schemaJsonRoot, schemaJsonPath, flagName, flagValueToInclude, flagValueDefault)

      val sourceSchema = schemaJsonRoot.getConfigList(schemaJsonPath).asScala.toList

      // iterate over the columns that have the flag set and select the distinct property values we are looking for
      val columnsWihProperty = sourceSchema filter {
        case column: Config => {
          if (columnsWitFlag.contains(column.getString("colName")) && column.hasPath(propertyName)) true else false
        }
      }

      val properties = columnsWihProperty map { case column: Config => column.getString(propertyName) } // return column with given property set

      properties.distinct
    }
    else {
      List.empty[String]
    }
  }

  protected def getSchemaDistinctPropertyValuesWithOtherPropertyAndFlag(
      schemaJsonRoot: Config,
      schemaJsonPath: String,
      propertyName: String,
      otherPropertyName: String,
      otherPropertyValue: String,
      flagName: String,
      flagValueToInclude: Boolean,
      flagValueDefault: Option[Boolean]): List[String] = {

    if ( schemaJsonRoot.hasPath(schemaJsonPath) ) {
      // These are the columns with flag set to the value to include
      val columnsWitFlag = getSchemaColNamesWithFlag(schemaJsonRoot, schemaJsonPath, flagName, flagValueToInclude, flagValueDefault)

      val sourceSchema = schemaJsonRoot.getConfigList(schemaJsonPath).asScala.toList

      // iterate over the columns that have the flag set and select the distinct property values we are looking for
      val columnsWihProperty = sourceSchema filter {
        case column: Config => {
          if (columnsWitFlag.contains(column.getString("colName")) && column.hasPath(propertyName)
            && column.hasPath(otherPropertyName) && column.getString(otherPropertyName) == otherPropertyValue) true else false
        }
      }

      val properties = columnsWihProperty map { case column: Config => column.getString(propertyName) } // return column with given property set

      properties.distinct
    }
    else {
      List.empty[String]
    }
  }

  protected def getSchemaDistinctPropertyValues(
      schemaJsonRoot: Config,
      schemaJsonPath: String,
      propertyName: String): List[String] = {

    if ( schemaJsonRoot.hasPath(schemaJsonPath) ) {
      val sourceSchema = schemaJsonRoot.getConfigList(schemaJsonPath).asScala.toList

      val columns = sourceSchema filter {
        case column: Config => if (column.hasPath(propertyName)) true else false
      }

      val keysWithDuplicates = columns map { case column: Config => column.getString(propertyName) }
      keysWithDuplicates.distinct
    }
    else {
      List.empty[String]
    }
  }

  /**
   * This method is not related to Schema, but related to CSV File options.
   * CSV File options are the ones defined by Spark, so they are any valid spark options.
   * In DW JSON files the CSV file options have specific format for different consumptions of CSV file, e.g., in the loader or comparer.
   *
   * @param configObjectAsOption - configuration component as object with fields inside of it. This is applicable to CSV file of Mongo DB options
   * @return - Map[String,String] . The values in the map coverted to strings so they can be used by Spark options method that takes a map of strings with strings values
   */
  protected def getConfigObjectFieldsAndValues(
      configObjectAsOption: Option[Config] ): Map[String,String] = {

    if (configObjectAsOption.isDefined) {
      val attributes = configObjectAsOption.get
        .entrySet()
        .asScala
        .map(e => e.getKey.stripPrefix("\"").stripSuffix("\"") -> { if( e.getValue.unwrapped().getClass.toString == "class java.util.ArrayList" ) e.getValue.unwrapped().asInstanceOf[java.util.ArrayList[String]].asScala.mkString else e.getValue.unwrapped().toString } )
        // .map(e => e.getKey -> e.getValue.unwrapped().getClass.toString ) // this was to determine the type of value
        .toMap

      val filteredOfComments = attributes.view.filterKeys(key => !key.startsWith( "_comment" ) ).toMap
      filteredOfComments
    }
    else {
      Map[String,String] ()
    }

  }

  def isColumnIntegerType(dt: DataType): Boolean = dt match {
    case ByteType | ShortType | IntegerType | LongType => true
    case _ => false
  }

}
