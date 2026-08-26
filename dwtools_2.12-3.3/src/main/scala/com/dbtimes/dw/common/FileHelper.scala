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
import java.util.{Calendar, Date}
import java.nio.file.{FileSystems} // only used to get the path separator
import java.util.UUID.randomUUID
import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.commons.io.FilenameUtils
import org.apache.spark.storage.StorageLevel

private [dw] object FileHelper {
  /**
   *
   * @param directory
   * @param isRecursive
   * @param isGetFilesOnly       - if set, files are included in the result
   * @param isGetDirectoriesOnly - if set, directories are included in the result, if both flags are set, files and directories are included.
   *                             if none set all files are included in the result (that may included links, etc. )
   * @param filterExpression     - default is true to return all files after the file category ( file, or directory, or both ) was applied
   * @return - list of full file names and/or directories, i.e., includes a path and a file/directory name
   */
  private def getFilesInDirectory(directory: String, isRecursive: Boolean, isGetFilesOnly: Boolean, isGetDirectoriesOnly: Boolean, filterExpression: String => Boolean = (filePath: String) => true): List[String] = {
    // list all files
    val spark = SparkSession.builder().getOrCreate() // this gets previously created session
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    var files: List[String] = List.empty
    var dirs: Set[String] = Set.empty
    val fileSeparator = FileSystems.getDefault().getSeparator()

    val iter = fs.listFiles(new HadoopPath(directory), isRecursive)
    while (iter.hasNext) {
      val nextFile = iter.next
      val nextFileAsString = nextFile.getPath.toString.replace(HadoopPath.SEPARATOR, fileSeparator)
      val relativePath = if (nextFileAsString.contains(directory)) nextFileAsString.drop(nextFileAsString.indexOf(directory) + directory.length + 1) else nextFileAsString
      val directoriesInTheMiddle = if (relativePath.endsWith(nextFile.getPath.getName)) relativePath.dropRight(nextFile.getPath.getName.length + 1) else relativePath

      if (isGetFilesOnly && nextFile.isFile) {
        if (!List(nextFile.getPath.getName).filter(filterExpression(_)).isEmpty) // the name is good based on filter
          files :+= FileHelper.makePath(directory, FileHelper.makePath(directoriesInTheMiddle, nextFile.getPath.getName))
      }
      if (isGetDirectoriesOnly) {
        if (!List(directoriesInTheMiddle).filter(filterExpression(_)).isEmpty) // the name is good based on filter
          dirs += FileHelper.makePath(directory, directoriesInTheMiddle)
      }
    }

    files ++ dirs.toList
  }

  /**
   *
   * @param sourceDir
   * @param destDir
   * @param filterExpression -- default is to move all files
   */
  private def moveDirectoryFiles(sourceDir: String, destDir: String, filterExpression: String => Boolean = (filePath: String) => true): Unit = {

    val fileSeparator = FileSystems.getDefault().getSeparator()
    val sourceDirWithNoLastSeparator = if (sourceDir.takeRight(1) == fileSeparator) sourceDir.substring(0, sourceDir.length - 1) else sourceDir
    val destDirWithNoLastSeparator = if (destDir.takeRight(1) == fileSeparator) destDir.substring(0, destDir.length - 1) else destDir

    val filesInSourceDirectory = FileHelper.getFilesInDirectory(sourceDirWithNoLastSeparator, false, true, false, filterExpression)

    val spark = SparkSession.builder().getOrCreate() // this gets previously created session
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    filesInSourceDirectory.foreach { fileName =>
      fs.rename(
        new HadoopPath(fileName), // source
        new HadoopPath(fileName.replace(sourceDirWithNoLastSeparator, destDirWithNoLastSeparator)))
    }
  }

  /**
   *
   * @param sourceDir
   * @param destDir
   */
  private [dw] def moveDirectory(sourceDir: String, destDir: String): Unit = {
    val spark = SparkSession.builder().getOrCreate() // this gets previously created session
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    if (isDirectoryOrFileExists(sourceDir)) {
      deleteDirectoryOrFileIfExists(destDir)
      fs.rename(
        new HadoopPath(sourceDir), // source
        new HadoopPath(destDir))
    }
  }

  def isDirectoryOrFileExists(dirOrFile: String): Boolean = {

    val spark = SparkSession.builder().getOrCreate() // this gets previously created session
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    fs.exists(new HadoopPath(dirOrFile))
  }

  private [dw] def deleteDirectoryOrFileIfExists(dirOrFile: String): Unit = {

    val spark = SparkSession.builder().getOrCreate() // this gets previously created session
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    if (fs.exists(new HadoopPath(dirOrFile))) fs.delete(new HadoopPath(dirOrFile), true)
  }

  private def isParquetFile(sourceFilePath: String): Boolean = {
    val fileName = FilenameUtils.getName(sourceFilePath)

    val fileNamePatternRegex = """.*\.parquet$""".r // pattern of parquet file spark creates
    if (fileNamePatternRegex.findFirstMatchIn(fileName).isDefined)
      true
    else
      false
  }

  private def isCsvFile(sourceFilePath: String): Boolean = {
    val fileName = FilenameUtils.getName(sourceFilePath)

    val fileNamePatternRegex = """.*\.csv$""".r // pattern of parquet file spark creates
    if (fileNamePatternRegex.findFirstMatchIn(fileName).isDefined)
      true
    else
      false
  }

  private [dw] def saveDataFrameAsParquet(
      df: DataFrame,
      filePath: String,
      filePathPrevVersion: String = null,
      columnsPartitionBy: List[String] = List.empty): Unit = {

    val spark = SparkSession.builder().getOrCreate() // this gets previously created session
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val uniqueId = randomUUID.toString

    if (columnsPartitionBy.isEmpty) {
      df.write
        .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
        .mode("overwrite")
        .save(filePath + uniqueId)
    } else {
      df.write
        .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
        .partitionBy(columnsPartitionBy: _*)
        .mode("overwrite")
        .save(filePath + uniqueId)
    }

    // Delete the old and rename the new
    // Check if Destination file exists before renaming. It will not exist on initial load
    if (filePathPrevVersion != null && fs.exists(new HadoopPath(filePath))) {
      // Delete prev version if exists
      fs.delete(new HadoopPath(filePathPrevVersion), true)
      fs.rename(
        new HadoopPath(filePath), // source
        new HadoopPath(filePathPrevVersion))
    }
    else
      fs.delete(new HadoopPath(filePath), true)

    fs.rename(
      new HadoopPath(filePath + uniqueId), // source
      new HadoopPath(filePath))
  }

  private [dw] def saveDataFrameAddToExisting(
      df: DataFrame,
      filePath: String,
      filePathPrevVersion: String = null,
      columnsPartitionBy: List[String] = List.empty): Unit = {

    val spark = SparkSession.builder().getOrCreate() // this gets previously created session
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val uniqueId = randomUUID.toString

    if (columnsPartitionBy.isEmpty) {
      df.write
        .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
        .mode("overwrite")
        .save(filePath + uniqueId)
    } else {
      df.write
        .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
        .partitionBy(columnsPartitionBy: _*)
        .mode("overwrite")
        .save(filePath + uniqueId)
    }

    // Save current version to prev version
    if (filePathPrevVersion != null && fs.exists(new HadoopPath(filePath))) {
      // Delete prev version if exists
      fs.delete(new HadoopPath(filePathPrevVersion), true)
      fs.copyFromLocalFile(false, true,
        new HadoopPath(filePath), // source
        new HadoopPath(filePathPrevVersion))
    }

    // Copy new files over the existing files. The assumption here is that the file names are unique,
    // so the new files will not replace any existing files
    if (fs.exists(new HadoopPath(filePath))) {

      //      val dir = FileSystems.getDefault.getPath(filePath + uniqueId)
      if (!columnsPartitionBy.isEmpty) {
        // partitioned data
        val newPartitions = FileHelper.getFilesInDirectory(filePath + uniqueId, true, false, true)
          .filter(_ != filePath + uniqueId) //  exclude the root path
          .map(_.replace(filePath + uniqueId, "")) // only take the part that has partitions. It will also have path separator as a first character

        newPartitions.foreach { newPartition =>
          if (!fs.exists(new HadoopPath(filePath + newPartition))) {
            fs.copyFromLocalFile(
              new HadoopPath(filePath + uniqueId + newPartition), // source
              new HadoopPath(filePath + newPartition))
          } else {
            val filesInExistingPartition = FileHelper.getFilesInDirectory(filePath + newPartition, false, true, false, isParquetFile)

            filesInExistingPartition.foreach { newFile =>
              fs.rename(
                new HadoopPath(newFile), // source
                new HadoopPath(newFile.replace(filePath + uniqueId, filePath)))
            }
          }
        }
      }
      else { // non-partitioned data
        FileHelper.moveDirectoryFiles(filePath + uniqueId, filePath, isParquetFile)
      }
      // delete new temporary directory

      fs.delete(new HadoopPath(filePath + uniqueId), true)
    } else { // file does not exist
      fs.rename(
        new HadoopPath(filePath + uniqueId), // source
        new HadoopPath(filePath))
    }

  }

  private [dw] def saveDataFrameAsParquetReplacePartitions(
      df: DataFrame,
      filePath: String,
      filePathPrevVersion: String = null,
      columnsPartitionBy: List[String]): Unit = {

    val spark = SparkSession.builder().getOrCreate() // this gets previously created session
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val uniqueId = randomUUID.toString

    // This is an expensive option - copy may take a lot of time
    if (filePathPrevVersion != null && fs.exists(new HadoopPath(filePath))) {
      // Delete prev version if exists
      fs.delete(new HadoopPath(filePathPrevVersion), true)
      FileHelper.moveDirectoryFiles(filePath, filePathPrevVersion)
    }

    df.write
      .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .partitionBy(columnsPartitionBy: _*)
      .mode("overwrite")
      .save(filePath + uniqueId)

    // Move partitions from the ones just saved from current file. They will only include different
    // partitions, thus preserving the ones that did not change
    // val dir = FileSystems.getDefault.getPath(filePath + uniqueId)
    val newPartitions = FileHelper.getFilesInDirectory(filePath + uniqueId, true, false, true)
      .filter(_ != filePath + uniqueId) //  exclude the root path
      .filter(_.count(_ == '=') == columnsPartitionBy.length) // only include paths at the bottom of the directory structure, that is with all partitioning keys
      .map(_.replace(filePath + uniqueId, "")) // only take the part that has partitions. It will also have path separator as a first character

    /*
        val newPartitions = Files.walk(dir).iterator().asScala
          .filter(Files.isDirectory(_)) //  here _ is an element of the Iterator of type JavaPath. JavaPath is an alias for java.nio.file.Path (to avoid a clash with Hadoop's rg.apache.hadoop.fs.Path). See import above
          .map(_.toString)
          .filter(_ != filePath + uniqueId) //  exclude the root path
          .filter( _.count( _ == '=' ) == columnsPartitionBy.length )  // only include paths at the bottom of the directory structure, that is with all partitioning keys
          .map(_.replace(filePath + uniqueId, "")) // only take the part that has partitions. It will also have path separator as a first character
          .toList
    */

    // For each partition delete one from current dir if exists and copy the new one
    newPartitions.foreach { newPartition =>
      FileHelper.deleteDirectoryOrFileIfExists(FileHelper.makePath(filePath, newPartition))
      FileHelper.moveDirectoryFiles(FileHelper.makePath(filePath + uniqueId, newPartition), FileHelper.makePath(filePath, newPartition)) // use default filter that will qualify each file
    }

    // delete new temporary directory
    FileHelper.deleteDirectoryOrFileIfExists(filePath + uniqueId)
  }

  private [dw] def saveDataFrameAsParquetAndMoveToParentDir(
      df: DataFrame,
      fileNamePrefix: String,
      fileDir: String): Unit = {

    val spark = SparkSession.builder().getOrCreate() // this gets previously created session

    val fileName = fileNamePrefix +
      new SimpleDateFormat("yyyyMMdd_hhmmss").format(Calendar.getInstance().getTime()) +
      randomUUID.toString // + ".parquet"

    df.write
      .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .mode("overwrite")
      .save(FileHelper.makePath(fileDir, fileName))

    // spark creates subdirectory and in this case we can move the parquet file
    // that only has one record, into parent directory to have a less involved directory structure
    FileHelper.moveDirectoryFiles(FileHelper.makePath(fileDir, fileName), fileDir, FileHelper.isParquetFile) // pass filter that will qualify only .parquet files, i.e., do not move .crc files

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    // Delete dir created by Spark
    fs.delete(new HadoopPath(FileHelper.makePath(fileDir, fileName)), true)
  }

  private [dw] def removePathEndingSeparator(dirName: String): String = {
    val fileSeparator = FileSystems.getDefault().getSeparator()
    if (dirName.endsWith(fileSeparator)) dirName.dropRight(fileSeparator.length) else dirName
  }

  def makePath(dirName: String, fileName: String): String = {
    val fileSeparator = FileSystems.getDefault().getSeparator()

    val dirNameWithoutEndingSeparator = FileHelper.removePathEndingSeparator(dirName)
    val fileNameWithoutLeadingSeparator = if (fileName.startsWith(fileSeparator)) fileName.drop(fileSeparator.length) else fileName

    dirNameWithoutEndingSeparator + fileSeparator + FileHelper.removePathEndingSeparator(fileNameWithoutLeadingSeparator)
  }

  private [dw] def sanitizeFileName(fileName: String): String =
    fileName.replaceAll("[\\?+=\\\\/\\<\\>\\[\\] ]", "_")
      .replace(' ', '_')
      .replace('?', '_')

  /**
   * fs.concat is not available on all OS, e.g., Windows, so cannot use for now
   *
   * @param destFilePath
   * @param srcDirectories
   */
  private [dw] def concatCsvFiles(destFilePath: String, srcDirectories: List[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate() // this gets previously created session

    val fileLists = for (srcDirectory <- srcDirectories;
                         fileList = FileHelper.getFilesInDirectory(srcDirectory, false, true, false, isCsvFile)
                         ) yield fileList

    val allFilePathsAsString = fileLists.flatten.toArray
    val allFileHadoopPaths = for (filePathAsString <- allFilePathsAsString)
      yield new HadoopPath(filePathAsString)

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    // Delete dir created by Spark
    fs.concat(new HadoopPath(destFilePath), allFileHadoopPaths)
  }

  private def moveFirstCsvFileInDirectory(destFilePath: String, srcDirectory: String): Unit = {
    val spark = SparkSession.builder().getOrCreate() // this gets previously created session

    val fileList = FileHelper.getFilesInDirectory(srcDirectory, false, true, false, isCsvFile)

    if (fileList.size > 0) {
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.rename(new HadoopPath(fileList.head), new HadoopPath(destFilePath))
      deleteDirectoryOrFileIfExists(srcDirectory)
    }
  }

  def isFileExistsAndEmpty(filePath: String): Boolean = {
    val spark = SparkSession.builder().getOrCreate() // this gets previously created session
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (isDirectoryOrFileExists(filePath) && fs.getFileStatus(new HadoopPath(filePath)).getLen == 0)
      true
    else
      false
  }

  private[dw] def createFileNameWithCurrentTimestamp(filePathDest: String, sourceFilePathOrUrl: String, defaultFileName: String): String = {
    val fileNameInUrlWithoutExtension =
      try {
        sourceFilePathOrUrl.split(Array('/', '\\'))
          .last
          .split('.')(0)
      }
      catch {
        case e: Exception => defaultFileName
      }

    val fileName = FileHelper.makePath(filePathDest,
      FileHelper.sanitizeFileName(fileNameInUrlWithoutExtension
        + " _"
        + new SimpleDateFormat("yyyyMMdd_hhmmss").format(Calendar.getInstance().getTime())
        + ".csv")
    )

    fileName
  }

  /**
   *
   * @param csvFileName - this file may or may not have an extension
   * @param df
   * @return
   */
  private [dw] def saveDataFrameAsCsv(csvFileName: String, df: DataFrame): String = {

    val csvFilePathWithoutExtension = if (csvFileName.endsWith(".csv")) csvFileName.dropRight(".csv".length) else csvFileName

    // The logic here is to save a file using spark which creates a new directory (will end on _data)
    // and then move the only .csv file to the parent directory.

    val resultFileName = csvFilePathWithoutExtension + ".csv"
    val resultFileDataDir = csvFilePathWithoutExtension + "_data"
    //    val resultFileHeaderDir = resultFileBase + "_header"  // when merging create header in a separate directory and merge with individual .csv files without  header records

    df.persist(StorageLevel.MEMORY_AND_DISK)

    df.coalesce(1) // so there is only one file is created
      .write.format("com.databricks.spark.csv")
      .option("header", true)
      .option("escape", "\"")
      .option("multiline", true )
      .mode("overwrite")
      .csv(resultFileDataDir)

    // Concat is not supported on all OS, so cannot use it for now
    //    FileHelper.concatCsvFiles( resultFileName, List( resultFileDataDir))
    FileHelper.moveFirstCsvFileInDirectory(resultFileName, resultFileDataDir)
    resultFileName
  }

  private [dw] def saveStringToNewFile(
      fileText: String,
      fileNamePrefix: String,
      fileDir: String,
      fileExtension: String): String = {

    val spark = SparkSession.builder().getOrCreate() // this gets previously created session

    val extension = if (fileExtension != "") "." + fileExtension else ""

    val fileName = FileHelper.sanitizeFileName(fileNamePrefix + "_" +
      new SimpleDateFormat("yyyyMMdd_hhmmss").format(Calendar.getInstance().getTime()) + "_" +
      randomUUID.toString + extension)
    val filePath = FileHelper.makePath(fileDir, fileName)

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val newFile = fs.create(new HadoopPath(filePath))

    newFile.writeBytes(fileText)
    newFile.close()

    filePath
  }

}
