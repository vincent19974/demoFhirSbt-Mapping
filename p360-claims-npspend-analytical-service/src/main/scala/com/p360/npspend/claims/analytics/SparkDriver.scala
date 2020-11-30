package com.p360.npspend.claims.analytics

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object SparkDriver extends Logging {

  def logSparkConf(): Unit = {
    //Function to print spark conf used for this application
    for (conf <- sparkConf) {
      log.info(conf.toString())
    }
  }

  log.info("Initializing Spark Session")

  val spark: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()

  //Pass all configurations to spark session from config file
  spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
  spark.sparkContext.setLogLevel("INFO")
  val sparkConf: Array[(String, String)] = spark.sparkContext.getConf.getAll
  logSparkConf()


}


