package com.p360.npspend.claims.analytics

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import scala.beans.BeanProperty
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.apache.spark.sql.types._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, struct, _}

import java.io.{ByteArrayOutputStream, IOException, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.internal.Logging
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.io.{BufferedSource, Source}
import scala.sys.process._
import org.apache.spark.sql.functions._

import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.Column
import org.apache.log4j.{Level, Logger}


object NpspendAnalyticalProcess extends Logging {

  //variables declaration
  var loadType, hiveDB, hiveTable,metadataTable, npspendReadBucketPath, npspendWriteBucketPath, startDateForHistoryLoad, oldDataEffectiveDate, oldDataExpiryDate, npspendOldCsvReadBucketPath, logLevel: String = ""
  var extractedMap = Map[String, Any]()
  var claimDatesList = List[String]()
  var numOfMonth = 1
  var effDate = Utils.getCurrentDate() //"2020-11-01" // Starting date for Incremental load

//stating main function
  def main(args: Array[String]): Unit = {

    startApplication(args)

    getConfValueFromMap()

    Logger.getRootLogger.setLevel(Level.toLevel(logLevel))

    calculateDatesAndGetAsList


    val spark = SparkDriver.spark

    if (loadType.equalsIgnoreCase("history")) {

      println("HISTORY PROCESS...")
      //EMRFS SYNCH BEFORE LOAD
      AWSUtils.runShellCmd("emrfs", npspendWriteBucketPath, metadataTable)

      //npspend old history file load
      readOldCsvFileAndPushtokafka(spark)

      //npspend claims history load
      readDataAndPushtokafka(spark)

      //EMRFS SYNCH AFTER LOAD
      AWSUtils.runShellCmd("emrfs", npspendWriteBucketPath, metadataTable)

      // npspend history hive load
      createHiveExternalTable(spark)

    } else {

      println("INCREMENTAL PROCESS...")

      //EMRFS SYNCH BEFORE LOAD
      AWSUtils.runShellCmd("emrfs", npspendWriteBucketPath, metadataTable)

      //npspend claims incremental load
      readDataAndPushtokafka(spark)

      //EMRFS SYNCH AFTER LOAD
      AWSUtils.runShellCmd("emrfs", npspendWriteBucketPath, metadataTable)

      //npspend history hive load
      createHiveExternalTable(spark)

    }
  }


  def startApplication(args: Array[String]): Unit = {

    println("Starting Application--------> \n ")
   
    
    args.foreach { println }
    if (!validArguments(args)) {
      throw new Exception("Invalid arguments.")
    }

    var common_env: String = args(0) //ex:- poc or prod
    println("common_env-------->"  + common_env)

    var npspendConfBucketPath = "s3://p360-" + common_env + "-generic/input-json/input_npspend.json"

    if (npspendConfBucketPath.contains("s3"))
      extractedMap = AWSUtils.getJsonConfigasMap(npspendConfBucketPath).asInstanceOf[Map[String, String]]
    else
      extractedMap = JsonExtractor.extractJson(npspendConfBucketPath, Utils.readFileFromLocation)

    println("npspendConfBucketPath ==>" + npspendConfBucketPath)
    println("extractedMap Values ===> " + extractedMap.mkString("\n"))
  }

  def validArguments(args: Array[String]): Boolean = {
    if (args.length < 1) {
      printUsage
      false
    }
    true
  }

  private def printUsage(): Unit = {
    println(
      """
     *************************************************************************************************************
     **    Np Spend Analytical Process                                                                          **
     **                                                                                                         **
     **                                                                                                         **
     **    Usage:                                                                                               **
     **           Expecting arguments :: <<env>> ex : nonprod or prod                                           **
     *************************************************************************************************************
					""".stripMargin)
  }


  //defining all the conf values to read from the configuration json file

  def getConfValueFromMap(): Unit = {
    loadType = extractedMap.get("loadType").map(_.toString).getOrElse("")
    hiveDB = extractedMap.get("hive.db.name").map(_.toString).getOrElse("")
    hiveTable = extractedMap.get("hive.table.name").map(_.toString).getOrElse("")
    metadataTable = extractedMap.get("metadata.table.name").map(_.toString).getOrElse("")
    npspendReadBucketPath = extractedMap.get("npspendReadBucketPath").map(_.toString).getOrElse("")
    npspendWriteBucketPath = extractedMap.get("npspendWriteBucketPath").map(_.toString).getOrElse("")
    startDateForHistoryLoad = extractedMap.get("startDateForHistoryLoad").map(_.toString).getOrElse("")
    npspendOldCsvReadBucketPath = extractedMap.get("npspendOldCsvReadBucketPath").map(_.toString).getOrElse("")
    oldDataEffectiveDate = extractedMap.get("oldDataEffectiveDate").map(_.toString).getOrElse("")
    oldDataExpiryDate = extractedMap.get("oldDataExpiryDate").map(_.toString).getOrElse("")
    numOfMonth = extractedMap.get("number.of.months").map(_.toString).getOrElse("0").toInt
    logLevel = extractedMap.get("log.level").map(_.toString).getOrElse("ERROR")

  }

  // Calculating the dates for 24 months for each effective date and getting the list and mapping it for each iteration

  def calculateDatesAndGetAsList(): Unit = {

    if (loadType.equalsIgnoreCase("history")) {
      numOfMonth = 24
      effDate = startDateForHistoryLoad // Starting date for history load
    }

    else {
      println("process is running for incremental")
      numOfMonth = 0
      effDate = effDate.substring(0 ,8).concat("01")
    }

    println("effDate ==> " + effDate)

    (0 to numOfMonth).toList.map(x => {

      var datestr = effDate
      val expDate = Utils.getlastDateofMonth(effDate)
      var npspndStartDate = Utils.getlastDateofMonth(Utils.calculateMonth(effDate, 1).toString())
      var npspndEndDate = Utils.calculateYear(npspndStartDate.toString(), 1)
      var getMonthAfter = Utils.calculateMonth(datestr, -1)

      claimDatesList = claimDatesList.+:(effDate + "," + expDate + "," + npspndStartDate + "," + npspndEndDate)

      effDate = getMonthAfter.toString()

    })

  }


  // Reading the old file data(2017-09-30), applying the aggregation and pushing it to s3 location and kafka topic

  def readOldCsvFileAndPushtokafka(spark: SparkSession): Unit = {

    println("Reading old CSV file")

    val npspendOldDf = spark.read.format("csv").option("header", "true")
      .option("delimiter",",").load(npspendOldCsvReadBucketPath)
      .filter(not(col("billed_amount") === "" && col("allowed_amount") === ""))
      .filter(not(col("billed_amount").cast("double").gt(0.0) && col("allowed_amount").cast("double").lt(0.0)))

    println("npspendOldDf")
    npspendOldDf.show(20,false)

//rejecting negative amounts, nulls, empty strings and data conversions.
val npspendOldFilterDf = npspendOldDf
      .withColumn("service_type", upper(col("service_type")))
      .withColumn("fund_type_code", when(col("fund_type_code") === "FULLY INSURED","1")
        .when(col("fund_type_code") === "ASO","2")
        .otherwise("3"))
      .withColumn("line_of_business_code", when(col("line_of_business_code") === "E&I","COM")
        .when(col("line_of_business_code") === "C&S","MCD")
        .when(col("line_of_business_code") === "M&R","MCR")
        .otherwise("UNKNOWN"))


    //common df - for both s3 and kafka
    val groupedOldDf1 = npspendOldFilterDf.groupBy("provider_tin_number", "line_of_business_code", "fund_type_code", "service_type")
      .agg(
        sum("billed_amount").cast("double").as("charge_amount"),
        sum("allowed_amount").cast("double").as("allowed_amount"),
        sum("allowed_amount").divide(sum("billed_amount")).cast("double").as("allowed_to_charge_ratio"))
      .withColumnRenamed("provider_tin_number", "source_system_provider_tin")
      .withColumnRenamed("line_of_business_code", "segment")
      .withColumnRenamed("fund_type_code", "funding_type")
      .withColumn("funding_type", col("funding_type").cast("string"))
      .withColumn("row_effective_date", from_unixtime(unix_timestamp(lit(oldDataEffectiveDate),"yyyy-MM-dd")).cast("DATE"))
      .withColumn("row_expiry_date", from_unixtime(unix_timestamp(lit(oldDataExpiryDate),"yyyy-MM-dd")).cast("DATE"))



    //kafka preparation
    val groupedOldDf2 = groupedOldDf1
      .withColumn("charge_amount", col("charge_amount").cast("string"))
      .withColumn("allowed_amount", col("allowed_amount").cast("string"))
      .withColumn("allowed_to_charge_ratio", col("allowed_to_charge_ratio").cast("string"))
      .withColumn("charge_amount", when(col("charge_amount") === "" || col("charge_amount") === null  ,"0.0").otherwise(col("charge_amount")))
      .withColumn("allowed_amount", when(col("allowed_amount") === "" || col("allowed_amount") === null  ,"0.0").otherwise(col("allowed_amount")))
      .withColumn("allowed_to_charge_ratio", when(col("allowed_to_charge_ratio") === "" || col("allowed_to_charge_ratio") === null  ,"0.0").otherwise(col("allowed_to_charge_ratio")))
      .withColumn("effective_month", substring(col("row_effective_date"),0,7))



    //new logic to convert the data to json format
    val kafkaFinalOldDf = groupedOldDf2.select(
      to_json(struct("source_system_provider_tin", "segment", "funding_type", "service_type", "effective_month")).alias("key"),
      to_json(struct("source_system_provider_tin", "segment", "funding_type", "service_type","effective_month","charge_amount", "allowed_amount","allowed_to_charge_ratio")).alias("value")
    )

    println("kafkaFinaloldDf")
    kafkaFinalOldDf.printSchema()
    //kafkaFinalOldDf.show




    // s3 push
    groupedOldDf1
      .repartition(50)
      .write
      .mode("overwrite")
      .partitionBy("row_effective_date") ///effective date partition
      .save(npspendWriteBucketPath)




    //kafka push
    println("Writing the old data to Kafka Topics for the Eff Date ==> " + oldDataEffectiveDate)
    KafkaJsonProducer.writeToKafka(kafkaFinalOldDf, extractedMap)

    println("completed old data kafka push for EffDate ==> " + oldDataExpiryDate)

  }



  // Reading the blue pipeline data, applying the aggregation and pushing it to s3 location and kafka topic

  def readDataAndPushtokafka(spark: SparkSession): Unit = {

    println("npspendReadBucketPath------->" + npspendReadBucketPath)

    val df = spark.read.parquet(npspendReadBucketPath)
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

    println("df...")
    df.show()

    //calculating
    claimDatesList.map(eachstring => {

      val eachstringarr = eachstring.split(",")

      val effDate = eachstringarr.apply(0)
      val expDate = eachstringarr.apply(1)
      val npspndStartDate = eachstringarr.apply(2)
      val npspndEndDate = eachstringarr.apply(3)

      println("EffDate ==> " + effDate) // "2018-10-01"
      println("Expiry Date  ==> " + expDate) // "2018-10-31"
      println("npspndStartDate  ==> " + npspndStartDate) // "2018-09-30"
      println("npspndEndDate  ==> " + npspndEndDate) //"2017-09-31"

      println("")


      //rejecting negative amounts, nulls, empty strings and data conversions.
      val filteredDf = df.filter(col("claim_status_code") === "A")
        .filter(col("first_service_date").gt(lit(npspndEndDate)) && col("first_service_date").leq(lit(npspndStartDate)))
        .filter(not(col("total_charge_amount") === 0.0 && col("total_allowed_amount") === 0.0))
        .filter(not(col("total_charge_amount").gt(0.0) && col("total_allowed_amount").lt(0.0)))


      //println("filteredDf")
      //filteredDf.show

      //common df for s3 and kafka
      val groupedDf1 = filteredDf.groupBy("provider_tin_number", "line_of_business_code", "fund_type_code", "service_type")
        .agg(
          sum("total_charge_amount").as("charge_amount"),
          sum("total_allowed_amount").as("allowed_amount"),
          sum("total_allowed_amount").divide(sum("total_charge_amount")).as("allowed_to_charge_ratio"))
        .withColumnRenamed("provider_tin_number", "source_system_provider_tin")
        .withColumnRenamed("line_of_business_code", "segment")
        .withColumnRenamed("fund_type_code", "funding_type")
        .withColumn("funding_type", col("funding_type").cast("string"))
        .withColumn("row_effective_date", from_unixtime(unix_timestamp(lit(effDate),"yyyy-MM-dd")).cast("DATE"))
        .withColumn("row_expiry_date", from_unixtime(unix_timestamp(lit(expDate),"yyyy-MM-dd")).cast("DATE"))



      println("groupedDf1")
      groupedDf1.printSchema()
      //groupedDf1.show



      //for kafka preparation
      val groupedDf2 = groupedDf1
        .withColumn("charge_amount", col("charge_amount").cast("string"))
        .withColumn("allowed_amount", col("allowed_amount").cast("string"))
        .withColumn("allowed_to_charge_ratio", col("allowed_to_charge_ratio").cast("string"))
        .withColumn("charge_amount", when(col("charge_amount") === "" || col("charge_amount") === null  ,"0.0").otherwise(col("charge_amount")))
        .withColumn("allowed_amount", when(col("allowed_amount") === "" || col("allowed_amount") === null  ,"0.0").otherwise(col("allowed_amount")))
        .withColumn("allowed_to_charge_ratio", when(col("allowed_to_charge_ratio") === "" || col("allowed_to_charge_ratio") === null  ,"0.0").otherwise(col("allowed_to_charge_ratio")))
        .withColumn("effective_month", substring(col("row_effective_date"),0,7))



      println("groupedDf2")
      groupedDf2.printSchema()
      //groupedDf2.show

      //new logic to convert the data to json format
      val kafkaFinalDf = groupedDf2.select(
        to_json(struct("source_system_provider_tin", "segment", "funding_type", "service_type", "effective_month")).alias("key"),
        to_json(struct("source_system_provider_tin", "segment", "funding_type", "service_type","effective_month","charge_amount", "allowed_amount","allowed_to_charge_ratio")).alias("value")
      )

      println("kafkaFinalDf")
      kafkaFinalDf.printSchema()
      //kafkaFinalDf.show

      //s3 push
      groupedDf1
        .repartition(50)
        .write
        .mode("overwrite")
        .partitionBy("row_effective_date")  //effective date partition
        .save(npspendWriteBucketPath)

      //kafka push
      println("Writing the data to Kafka Topics for the Eff Date ==> " + effDate)
      KafkaJsonProducer.writeToKafka(kafkaFinalDf, extractedMap)
      println("completed kafka push for EffDate ==> " + effDate)
    })
  }


  // Creating the Hive table

  private def createHiveExternalTable(spark: SparkSession): Any = {

    println("*** creating hive external table *** ")

    spark.sql(s"""  use ${hiveDB}""")
    spark.sql(s""" SET hive.exec.dynamic.partition = true""")
    spark.sql(s""" SET hive.exec.dynamic.partition.mode = nonstrict""")
    spark.sql(s""" SET hive.mapred.mode = nonstrict""")

    spark.sql(s""" DROP TABLE IF EXISTS ${hiveDB}.${hiveTable} """)
    spark.sql(s"""
       CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveDB}.${hiveTable}(
  `source_system_provider_tin` string,
  `segment` string,
  `funding_type` string,
  `service_type` string,
  `charge_amount` double,
  `allowed_amount` double,
  `allowed_to_charge_ratio` double,
  `row_expiry_date` date
   )
  PARTITIONED BY (
  `row_effective_date` date)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${npspendWriteBucketPath}'

      """)
      
      spark.sql(s""" MSCK REPAIR TABLE ${hiveDB}.${hiveTable}""")

    println("Ran MSCK REPAIR TABLE: " + DateApp.getDateTime())
    println("external hive table created: " + DateApp.getDateTime())
  }

  private def alterHiveTableWithPartitions(spark: SparkSession): Any = {


  }
}






