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

import java.nio.charset.Charset
import java.nio.charset.CodingErrorAction
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import java.net.URL

import scala.io.Source
import scala.jdk.CollectionConverters._

import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}
import org.apache.spark.sql.SparkSession
import org.apache.commons.codec.binary.Base64

import com.dbtimes.dw.common._

private[sourceloader] trait ProcessFileSourceAny {

  // includeDirectories is not currently implemented
  private[sourceloader] def getFilesToProcessSortedByEffectiveDate(action: LoadAction, includeDirectories: Boolean = false): List[String] = {
    // list all applicable files that match the pattern and process them
    // val dir = FileSystems.getDefault.getPath(action.getSourceFileDir )

    //    List( "/dw/data/SOURCES/TeamInfo/NFL-Teams.csv" )

    // List of regular files (not directories) whose names match the pattern defined in the configuration
    val spark = SparkSession.builder().getOrCreate() // this gets previously created session
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    var files: List[String] = List.empty

    val iter = fs.listFiles(new HadoopPath(action.getSourceFileDir), false)
    while (iter.hasNext) {
      val nextFile = iter.next
      if (nextFile.isFile) {
        files = files :+ FileHelper.makePath(action.getSourceFileDir, nextFile.getPath.getName)
      }
    }

    val filesWithOptionalEffectiveDates = files
      .filter(isFileMatchesPattern(_, action))
      // passing a parameter this way is using scala currying. The other way to write it is
      // .map(associateFileEffectiveDateWithFileName(action)(_)) with the second list containing the element of the Iterator with file names.
      // Curried functions are useful when one argument function is expected, which is the case here where an argument to map is a function that takes one parameter
      .map(associateFileEffectiveDateWithFileName(action))
    //      .toList

    // Finally sort by effective dates. If the file does not have effective
    // date in it name it does not matter if it is sorted first or last
    // this is a list of ( Date, String ) - effectiveDate, filename
    filesWithOptionalEffectiveDates.sortBy(_._1).map(effDate_fileName => effDate_fileName._2) // create list of file names only
  }

  private def isFileMatchesPattern(sourceFile: String, action: LoadAction): Boolean = {
    val fileName = FilenameUtils.getName(sourceFile)

    val fileNamePatternRegex = action.getSourceFileNamePattern.r // pattern
    if (fileNamePatternRegex.findFirstMatchIn(fileName).isDefined)
      true
    else
      false

    /*  this syntax did not work on Named Capturing Group patterns
        val isMatch = fileName match {
          case `fileNamePattern` => {  // ` is needed because fileNamePattern is a variable. and otherwise you get warning 'unreachable code due to variable patte  rn'
          ....
          }
          case _ => {
            loaderLog.info("Skipping processing file " + fileName + " - did not match the name pattern.")   // file name can have Named Capturing Groups
            false
          }
        }*/
  }

  private def associateFileEffectiveDateWithFileName(action: LoadAction)(sourceFile: String): (Date, String) = {
    val optionalEffectiveDate = action.getEffectiveDate(sourceFile)
    (optionalEffectiveDate.getOrElse(new SimpleDateFormat("yyyyMMdd").parse("19000101")), sourceFile.toString)
  }

  private[sourceloader] def moveFileToProcessedDir(action: LoadAction, sourceFilePath: String): Unit = {
    if (action.getProcessedFilesDir.isDefined) {
      val fileName = FilenameUtils.getName(sourceFilePath)
      val processedFileName = action.getProcessedFilesDir.get +
        fileName +
        "_processed_" +
        new SimpleDateFormat("yyyyMMdd_hhmmss").format(Calendar.getInstance().getTime())

      val spark = SparkSession.builder().getOrCreate() // this gets previously created session
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.rename(
        new HadoopPath(sourceFilePath), // source
        new HadoopPath(processedFileName))
    }
  }

  def copyInternetFileToTempFile( action: LoadAction, fileExtension: String ): String = {

    if ( action.getIsDisableSslVerification.isDefined && action.getIsDisableSslVerification.get == true )
      SSLSecurityBypasser.destroySSLSecurity()

    val fileAsString = try {
      if (action.getSourceFileUser.isDefined && action.getSourceFilePassword.isDefined) {
        val AUTHORIZATION = "Authorization"

        val decoder = Charset.forName("UTF-8").newDecoder()
        decoder.onMalformedInput(CodingErrorAction.IGNORE)

        val connection = new URL(action.getSourceFileUrl).openConnection
        connection.setRequestProperty(AUTHORIZATION, getHeader(action.getSourceFileUser.get, action.getSourceFilePassword.get))
        val response = Source.fromInputStream(connection.getInputStream)(decoder)

        response.mkString
      }
      else {
        val html = Source.fromURL(action.getSourceFileUrl)
        html.mkString
      }
    }
    finally {
      if ( action.getIsDisableSslVerification.isDefined && action.getIsDisableSslVerification.get == true )
        SSLSecurityBypasser.restoreSSLSecurity()
    }

    FileHelper.saveStringToNewFile( fileAsString, "internet_file", action.getProcessedFilesDir.get, fileExtension )
  }

  def encodeCredentials(username: String, password: String): String = {
    new String(Base64.encodeBase64String((username + ":" + password).getBytes))
  }

  def getHeader(username: String, password: String): String =
    "Basic" + " " + encodeCredentials(username, password)

}
