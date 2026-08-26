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
import scala.util.matching.Regex
// import scala.util.chaining._   // Scala 2.13
import java.util.{Calendar, Date}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Dataset, DataFrame, Row, SparkSession}
import LogFile.{dwlogger => loaderLog}

import com.opencsv.{CSVParserBuilder, CSVReaderBuilder, RFC4180ParserBuilder, RFC4180Parser, CSVWriter, CSVWriterBuilder }
import java.io.{StringReader,StringWriter}
import scala.collection.JavaConverters._
// import scala.jdk.CollectionConverters._ // Scala 2.13
import scala.collection.mutable.ListBuffer

private[sourceloader] class ProcessorOfCsvFileSource(private val action: LoadAction) extends ActionProcessor
  with ProcessFileSourceAny
  with SourceDataMerger {

  private[sourceloader] def process(): Unit = {

    if ( action.getIsFileSourceFileSystemLocation ) {
      // get a list of all files that match the pattern sorted by effective date and process them
      val fileList = getFilesToProcessSortedByEffectiveDate(action)
      fileList.foreach(processFile)
    }
    else if ( action.getIsFileSourceInternetLocation ) {
      val filePath = copyInternetFileToTempFile( action, "csv" )
      processFile( filePath )
    }
    else if ( action.getIsFileSourceCustomLocation ) {
      val filePathOrData = getFileFromCustomLocation( )
      processFile( filePathOrData )
    }
  }

  private[sourceloader] def getFileFromCustomLocation( ): String = {
    val packageName = action.getSourceFilePackageName
    val moduleName  = action.getSourceFileModuleName
    val methodName  = action.getSourceFileMethodName

    val invoker = MiscHelper.getInvokerForDynamicMethodInvokation( packageName, moduleName, methodName )

    val applicationSpecificConfigAsOption = action.getSourceFileApplicationSpecific

    // Invoke the method with arguments
    val filePathOrData = invoker( applicationSpecificConfigAsOption ).asInstanceOf[String]
    filePathOrData
  }

  private def splitCsvString( csvString: String, escapeCharacter: Char, isMultilineCsvData: Boolean ): Array[String] = {

    val rawRows = ListBuffer[String]()

    val parser = new RFC4180ParserBuilder()
      // .withSeparator(',')
      // .withQuoteChar('"')
      .build()

    val reader = new CSVReaderBuilder(new StringReader(csvString))
      .withCSVParser(parser)
      .build()

    try {
      var nextLine = reader.readNext()
      while (nextLine != null) {
        val stringWriter = new StringWriter()
        val writer = new CSVWriterBuilder(stringWriter)
          .withEscapeChar( escapeCharacter )
          .build()

        try {
          writer.writeNext(nextLine, false)
        } finally {
          writer.close()
        }

        rawRows += stringWriter.toString.trim
        nextLine = reader.readNext() // Read next line for loop condition
      }
    } finally {
      reader.close()
    }

    // Convert the ListBuffer to a flat 1D Array[String]
    val arrayOfRows: Array[String] = rawRows.toArray
    arrayOfRows

/*
    val rows: List[Array[String]] = reader.readAll().asScala.toList
    rows.map(_.mkString(",")).toArray
*/

  /*
    // " matches the opening double quote.
    // [^"\\]* matches any character that is not a quote or a backslash.
    // (?:""[^"\\]*)* is a non-capturing group. It matches a pair of double quotes ""
    //    (the CSV escape sequence) followed by any non-quote characters, repeated zero or more times.
    // " matches the closing double quote.
    val regex: Regex = if ( escapeCharacter == '"' ) {
      """"([^"]*(?:""[^"]*)*)"""".r
      // """"([^"\\]*(?:""[^"\\]*)*)"""".r
    } else if ( escapeCharacter == '\\' )
      """"([^"\\]*(?:\\.[^"\\]*)*)"""".r
      // """"(?:[^"\\]|\\.)*"""".r
    else
      s"""(?:\\$escapeCharacter")|("(?:[^"]|\\$escapeCharacter")*")|([^,"]+)""".r  // TODO fix this to look similar to the other two

    val result = if (isMultilineCsvData)
      regex.replaceAllIn(csvString, m => {

        val replaced = if ( escapeCharacter == '"' || escapeCharacter == '\\'  ) {
          // m.matched contains the entire quoted string including the outer quotes
          m.matched
            .replace("\r", "R:@#!-r")
            .replace("\n", "N:@#!-n")
        }
        else {
          if (m.group(1) != null) {
            // We found a double-quoted block. Replace the target char inside it.
            m.group(1)
              .replace("\r", "R:@#!-r")
              .replace("\n", "N:@#!-n")
          } else {
            // Keep escaped quotes and outside text exactly as they are
            m.matched
          }
        }
        Regex.quoteReplacement(replaced)
    })
    else
      csvString


    val splitCsv = result.split("\\r?\\n")

    // restore back \r and \n

    val restoredSplitCsv = if ( isMultilineCsvData)
      splitCsv.map( elem => elem.replace( "R:@#!-r", "\r" ).replace( "N:@#!-n", "\n" ) )
    else
      splitCsv

    restoredSplitCsv
  */

  }


  private[sourceloader] def processFile(sourceFilePathOrData: String): Boolean = {
    val spark = SparkSession.builder().getOrCreate() // this gets previously created session

    val isPassedValueFilePath = if( ( action.getIsFileSourceCustomLocation && action.getIsSourceFileMethodReturnsFilePath ) || !action.getIsFileSourceCustomLocation ) true else false
    val isPassedValueFileData = if( action.getIsFileSourceCustomLocation && action.getIsSourceFileMethodReturnsFileData ) true else false
    val lenForLogging = if (isPassedValueFileData) math.min( sourceFilePathOrData.length - 1, 100 ) else 0

    val timeStart = Calendar.getInstance().getTimeInMillis()
    try {
      val csvOptions = action.getCsvOptions

      if ( isPassedValueFilePath )
        loaderLog.info("** Processing file " + sourceFilePathOrData)
      else if ( isPassedValueFileData )
        loaderLog.info("** Processing file from string: " + sourceFilePathOrData.substring( 0, lenForLogging ) + "...")
      else {
        throw new RuntimeException("""Loader Configuration ERROR: unknown file data in processFile method  """)
      }

      val dfNewData = if ( isPassedValueFilePath ) {

        // Scala 2.13
/*        spark
          .read.format("com.databricks.spark.csv")
          .options( csvOptions )
          .pipe(q => if ( !(csvOptions.contains("inferSchema") && csvOptions.get("inferSchema").get.toBoolean == true) ) q.schema(action.getSchema) else q) // The optional call
          .load(sourceFilePathOrData)*/

        // Scala 2.12
        if ( !(csvOptions.contains("inferSchema") && csvOptions("inferSchema").toBoolean == true) ) {
          spark
            .read.format("com.databricks.spark.csv")
            .options( csvOptions )
            .schema(action.getSchema)
            .load(sourceFilePathOrData)
        }
        else {
          spark
            .read.format("com.databricks.spark.csv")
            .options( csvOptions )
            .load(sourceFilePathOrData)
        }
      }
      else if ( isPassedValueFileData ) {
        import spark.implicits._

        val splitString = splitCsvString( sourceFilePathOrData,
          if ( csvOptions.contains("escape") ) csvOptions("escape").charAt(0) else '\\',  // Spark default
          if ( csvOptions.contains("multiLine") ) csvOptions("multiLine").toBoolean else false
         )

        // split a string on new line unless the new line inside non-escaped double quotes
        val ds: Dataset[String] = splitString
          .toSeq
          .toDS()

        // Scala 2.13
/*        val df = spark
          .read // .format("com.databricks.spark.csv")
          .options( csvOptions )
          // .option("quote", "\"")     // this is default, can be overwritten in config
          // .option("escape", "\"")     // escape should be defined if it is not the default escape character
          // .option("multiLine", true)  // multiLine should be defined in configuration
          .pipe(q => if ( !(csvOptions.contains("inferSchema") && csvOptions.get("inferSchema").get.toBoolean == true) ) q.schema(action.getSchema) else q) // The optional call
          .csv(ds)*/

        // Scala 2.12
        val df = if ( !(csvOptions.contains("inferSchema") && csvOptions("inferSchema").toBoolean == true) )
          spark
            .read // .format("com.databricks.spark.csv")
            .options( csvOptions )
            // .option("quote", "\"")     // this is default, can be overwritten in config
            // .option("escape", "\"")     // escape should be defined if it is not the default escape character
            // .option("multiLine", true)  // multiLine should be defined in configuration
            .schema(action.getSchema) // optional call
            .csv(ds)
        else
          spark
            .read // .format("com.databricks.spark.csv")
            .options( csvOptions )
            // .option("quote", "\"")     // this is default, can be overwritten in config
            // .option("escape", "\"")     // escape should be defined if it is not the default escape character
            // .option("multiLine", true)  // multiLine should be defined in configuration
            .csv(ds)

        val fileName = FileHelper.createFileNameWithCurrentTimestamp( action.getProcessedFilesDir.get, action.getDestinationFilePath, "custom_csv_file" )
        FileHelper.saveDataFrameAsCsv(fileName, df)

        df
      }
      else {
        throw new RuntimeException("""Loader Configuration ERROR: unknown file data in processFile method  """)
      }

      if (action.getIsDebugDwLib) {
        dfNewData.show(60)
      }

      val effectiveDate = action.getEffectiveDate(dfNewData, if ( isPassedValueFilePath ) sourceFilePathOrData else "" )
      val dfMerged = mergeSourceData(action, dfNewData, effectiveDate)
      val dfPostProcessed = postProcess( action, dfMerged)
      val ( dfPostProcessedObserved, observationStatsAsOption ) = createLoadControlRecordObservation( action, timeStart, effectiveDate, dfPostProcessed )
      if (action.getIsFileDestination)
        saveMergedFile(action, dfPostProcessedObserved)
      else
        throw new RuntimeException("""Loader Configuration ERROR: Destination is not a file """)
      createLoadControlRecordFromFileDestinationParquet(action, timeStart, effectiveDate, isSuccess = true, errorMessage = "", observationStatsAsOption )

      if ( isPassedValueFilePath ) {
        moveFileToProcessedDir(action, sourceFilePathOrData)
        loaderLog.info("** Completed processing file " + sourceFilePathOrData)
      }
      else if ( isPassedValueFileData ) {
        loaderLog.info("** Completed processing file from string: " + sourceFilePathOrData.substring( 0, lenForLogging ) + "...")
      }
      true
    }
    catch {
      case e: Exception => {
        createLoadControlRecordFromFileDestinationParquet(action, timeStart, effectiveDate = null, isSuccess = false, e.getMessage)
        throw new Exception(e)
      }
    }
  }
}
