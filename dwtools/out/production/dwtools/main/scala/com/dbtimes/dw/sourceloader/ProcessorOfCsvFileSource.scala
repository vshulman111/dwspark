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
import scala.reflect.runtime.universe._
import java.util.{Calendar, Date}
import org.apache.spark.sql.SparkSession
import LogFile.{logger => loaderLog}

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

    val applicationSpecificConfigAsOption = action.getSourceFileApplicationSpecific

    // Invoke the method with arguments
    val filePathOrData = invoker( applicationSpecificConfigAsOption ).asInstanceOf[String]
    filePathOrData
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
        spark
          .read.format("com.databricks.spark.csv")
          .options( csvOptions )
          .schema(action.getSchema)
          .load(sourceFilePathOrData)
      }
      else if ( isPassedValueFileData ) {
        import spark.implicits._
        val ds = sourceFilePathOrData.split( "\n" ).toSeq.toDS()

        spark
          .read.format("com.databricks.spark.csv")
          .options( csvOptions )
          .schema(action.getSchema)
          .csv(ds)
      }
      else {
        throw new RuntimeException("""Loader Configuration ERROR: unknown file data in processFile method  """)
      }

      if (action.getSdaIsDebugDwLib) {
        dfNewData.show(60)
      }

      val effectiveDate = action.getEffectiveDate(dfNewData, if ( isPassedValueFilePath ) sourceFilePathOrData else "" )
      mergeSourceData(action, dfNewData, effectiveDate, loaderLog)
      if ( isPassedValueFilePath )
        moveFileToProcessedDir(action, sourceFilePathOrData)

      //    val processedFilePath =
      //      + new SimpleDateFormat("yyyyMMdd_hhmmss").format(Calendar.getInstance().getTime())
      //    + java.util.UUID.randomUUID.toString
      if ( isPassedValueFilePath )
        loaderLog.info("** Completed processing file " + sourceFilePathOrData)
      else if ( isPassedValueFileData )
        loaderLog.info("** Completed processing file from string: " + sourceFilePathOrData.substring( 0, lenForLogging ) + "...")
      createLoadControlRecordFromFileDestinationParquet(action, timeStart, effectiveDate, isSuccess = true, "")
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
