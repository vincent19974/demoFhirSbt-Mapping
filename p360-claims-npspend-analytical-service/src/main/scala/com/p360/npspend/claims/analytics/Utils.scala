package com.p360.npspend.claims.analytics

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.time.LocalDate
import java.util.Calendar
import java.util.TimeZone
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.FormatStyle

import scala.io.Source
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.functions.col
object Utils extends Logging {

  def jobStatusFlagsUpdate(statusMap: Map[String, Any]): Map[String, Any] = {
    statusMap map {
      case (a, false) => a -> "NOT-PERFORMED"
      case (a, true) => a -> "FAILED"
      case x => x
    }
  }

  def subjectFlagUpdate(subjectFlag: Map[String, Any]): Map[String, Any] = {
    subjectFlag map {
      case (a, false) => a -> "PROCESSED"
      case (a, true) => a -> "FAILED"
      case x => x
    }
  }


  def mapUpdate(mapFlags: Map[String, Any], key: String, value: Any): Map[String, Any] = {
    //Fucntion to updaate the flag status in interemediately
    mapFlags + (key -> value)
  }

  def convertListToMap(jobStatusFlags: List[String], value: Any): Map[String, Any] = {
    //Function to create a map from list for all the operations in spark job with default boolean FALSE
    jobStatusFlags.zip(jobStatusFlags.map(x => value)).toMap
  }

  def convertStrToList(argument2: String): List[String] = {
    //Function to take arguement 2 and convert to list of String(metricstypes)
    if (argument2 == "ALL") {
      val allSubjects = List("LAB", "RX", "REFERRAL", "RAD", "SURG") // GET this from prop file
      val arg2 = allSubjects
      arg2
    }
    else if (argument2 == "") {
      logInfo("Wrong Arguments Passed.. ENDING PROCESS")
      val arg2 = "NULL".split(",").toList
      arg2
    }
    else {
      val arg2 = argument2.split(",").toList
      arg2
    }
  }

  def readCSV(spark: SparkSession,inputPath: String): DataFrame = {
    //Functon to read csv file and return dataframe
    val dataDF = spark.read.format("csv").option("header", "true").option("delimiter", ",").load(inputPath)
    dataDF
  }


  def readAvro(spark: SparkSession,inputPath: String): DataFrame = {
    //Function to read avro file and return dataframe
    val dataDF = spark.read.format("avro").load(inputPath)
    dataDF
  }


  def writeToS3(inpuDF: DataFrame, ouputPath: String, format: String, mode: String): Boolean = {
    //Function to write spark dataframe
    inpuDF.write.format(format).option("header", "true").mode(mode).save(ouputPath)
    false
  }

  def writeToS3Partitioned(inpuDF: DataFrame, ouputPath: String, format: String, mode: String, partitionByCol1: String, partitionByCol2: String, partitionByCol3: String): Boolean = {
    //Function to write spark dataframe
    inpuDF.write.format(format).option("header", "true").partitionBy(partitionByCol1, partitionByCol2, partitionByCol3).mode(mode).save(ouputPath)
    false
  }


  def renameColandCast(inputDF: DataFrame, ColumnsJson: Map[String, String]): DataFrame = {
    //Function to rename columns from json file and cast them dynamically
    val inputSelectColDF = inputDF.select(ColumnsJson.keys.toSeq.map(col): _*)
    val inputRenamedColDF = inputSelectColDF.select(
      inputSelectColDF.columns.map(c =>
        col(c).as(ColumnsJson.getOrElse(c, c).split(",")(0))
      ): _*
    )
    val finaCastedDF = inputRenamedColDF.select(ColumnsJson.map {
      case (s, coltype) =>
        col(coltype.split(",")(0)).cast(coltype.split(",")(1))
    }.toList: _*)
    finaCastedDF
  }


  def filterRejectsSubjects(inputDF: DataFrame, failedSubjectList: List[String]): DataFrame = {
    val subjectMeasureNumbers = Map("REFERRAL" -> 1, "RX" -> 2, "LAB" -> 3, "RAD" -> 4, "SURG" -> 5)
    var filterDF = inputDF
    for (subject <- failedSubjectList) {
      filterDF = filterDF.filter(inputDF("network_measure_number") =!= subjectMeasureNumbers(subject.mkString))
    }
    filterDF

  }

  def renameCols(inputDF: DataFrame, inputMapCols: Map[String, String]): DataFrame = {
    //Function to rename dataframe columns from map
    val inputRenamedColDF = inputDF.select(inputDF.columns.map(c => col(c).as(inputMapCols.getOrElse(c, c))): _*)
    inputRenamedColDF
  }

  def executionTime(jobStartTime: java.sql.Timestamp): String = {
    val jobEndTime = java.sql.Timestamp.valueOf(java.time.LocalDateTime.now())
    logInfo("Job   End Time is : " + jobEndTime)
    val seconds = ((java.sql.Timestamp.valueOf(java.time.LocalDateTime.now()).getTime - jobStartTime.getTime) / 1000).toInt
    import scala.concurrent.duration._
    val inTime = Duration(seconds, SECONDS)
    "%02d:%02d:%02d".format(inTime.toHours, inTime.toMinutes % 60, inTime.toSeconds % 60)
  }
  
  def calculateDates(DataDate: String, dateNumber: Int): LocalDate = {
    val localDate = LocalDate.parse(DataDate)
    return localDate.minusDays(dateNumber)
  }
  
  def calculateMonth(DataDate: String, monthNumber: Int): LocalDate = {
    val localDate = LocalDate.parse(DataDate)
    return localDate.minusMonths(monthNumber)
  }
  
  
  def calculateYear(DataDate: String, yearNumber: Int): LocalDate = {
    val localDate = LocalDate.parse(DataDate)
    return localDate.minusYears(yearNumber)
  }
  
  def getlastDateofMonth(DataDate: String): LocalDate = {
   var convertedDate = LocalDate.parse(DataDate, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    convertedDate.withDayOfMonth(
                                convertedDate.getMonth().length(convertedDate.isLeapYear()));
  }
  
 def checkDigit(digit1: Int): Boolean = {
    var digit = digit1.toString
    digit.size.==(1) 
  }


  def getCurrentDate(): String = {
    import java.text.SimpleDateFormat
    import java.util.Date
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val date = new Date()
    formatter.format(date)
  }

  def disableVerboseLogging(disable: Boolean): Unit = {
    import org.apache.log4j.{ Level, Logger }
    if (disable) {

      //logInfo  "disabling verbose logging"
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
    }
  }


  def getDate(cal :Calendar ){
       cal.get(Calendar.DATE) +"-" +
                (cal.get(Calendar.MONTH)+1) + "-" + cal.get(Calendar.YEAR);
    }

  def readFileFromLocation(filename: String) : String = {
    println(s"Trying to read file from: ${filename}")
    //    val value = Source.fromFile(new URI(filename))
    val value = Source.fromFile(filename)
    println(s"Source : ${value}")
    val finalString = value.getLines().mkString
    println(s"Got final String: ${finalString}")
    value.close()
    finalString
  }



  def flattenDataframe(df: DataFrame): DataFrame = {

    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)
    val length = fields.length

    for(i <- 0 to fields.length-1){
      val field = fields(i)
      val fieldtype = field.dataType
      val fieldName = field.name
      fieldtype match {
        case arrayType: ArrayType =>
          val fieldNamesExcludingArray = fieldNames.filter(_!=fieldName)
          val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName) as $fieldName")
          // val fieldNamesToSelect = (fieldNamesExcludingArray ++ Array(s"$fieldName.*"))
          val explodedDf = df.selectExpr(fieldNamesAndExplode:_*)
          return flattenDataframe(explodedDf)
        case structType: StructType =>
          val childFieldnames = structType.fieldNames.map(childname => fieldName +"."+childname)
          val newfieldNames = fieldNames.filter(_!= fieldName) ++ childFieldnames
          val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_"))))
          val explodedf = df.select(renamedcols:_*)
          return flattenDataframe(explodedf)
        case _ =>
      }
    }
    df
  }

}


