package sample

import org.apache.spark.sql.SparkSession

class SparkUtils {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("demo-partition-fhir")
    .getOrCreate()
}
