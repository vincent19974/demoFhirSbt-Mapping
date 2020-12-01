package com.p360.npspend.claims.analytics

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, struct, _}
import za.co.absa.abris.avro.format.SparkAvroConversions
import za.co.absa.abris.avro.functions.to_confluent_avro
import za.co.absa.abris.avro.read.confluent.SchemaManager

object KafkaProducer extends Logging {

  def dataPush(inputDF: DataFrame, subjectName: String, kafkaConf: Map[String, String], kafkaKeyCols: Seq[String],
               kafkaValueCols: Seq[String]

              ): Unit = {
    logInfo("Performing Kafka datapush for :" + inputDF)

    logInfo("Loading configurations from conf file")
    //Initializing configuration for Kafka Source Schema Registey
    val (keyRegistryConfig, valueRegistryConfig) = kafkaProducerConf(subjectName, kafkaConf)
    val kafkaDF = inputDF.withColumn("key", concat(kafkaKeyCols.map(c => col(c)): _*))
    //Generating Avro Schema
    val avroValueSchema = SparkAvroConversions
      .toAvroSchema(kafkaDF.select(kafkaValueCols.toSeq.map(c => col(c)): _*).schema, "value", "")
      .toString
    val avroKeySchema = SparkAvroConversions
      .toAvroSchema(kafkaDF.select(kafkaKeyCols.toSeq.map(c => col(c)): _*).schema, "key", "")
      .toString
    //Constructing struct from argument column list -- For column sequence(PRODUCER KEY)
    logInfo("Completed loading configurations from conf file, proceeding..")

    //Performing dataPush to Kafka using ABRIS Package
    kafkaDF
      .select(to_confluent_avro(struct(kafkaKeyCols.head, kafkaKeyCols.tail: _*), avroKeySchema, keyRegistryConfig) as 'key,
        to_confluent_avro(struct(kafkaValueCols.head, kafkaValueCols.tail: _*), avroValueSchema, valueRegistryConfig) as 'value)
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaConf.get("bootStrapserver").mkString)
      .option("topic", kafkaConf.get(subjectName + "Topic").mkString)
      .option("kafka.security.protocol", kafkaConf.get("securityProtocol").mkString)
      .option("kafka.ssl.enabled.protocols", kafkaConf.get("sslProtocols").mkString)
      .option("ssl.endpoint.identification.algorithm", kafkaConf.get("identificationAlgorithm").mkString)
      .option("kafka.buffer.memory", 33554432)
      .option("kafka.max.request.size", 33554432)
      .option("kafka.linger.ms", 50)
      .option("kafka.producer.retries", 5)
      .option("kafka.max.block.ms", 7000000)
      .option("kafka.retry.backoff.ms", 1000)
      .option("kafka.request.timeout.ms", 7000000)
      .option("kafka.compression.type", "gzip")
      .save()
  }


  def kafkaProducerConf(subjectName: String, kafkaConf: Map[String, String]): (Map[String, String], Map[String, String]) = {
    val commonRegistryConfig = Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> kafkaConf
        .get(subjectName + "Topic")
        .mkString,
      SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> kafkaConf
        .get("schemaRegistryUrl")
        .mkString,
      SchemaManager.PARAM_VALUE_SCHEMA_NAME_FOR_RECORD_STRATEGY -> "RecordName",
      SchemaManager.PARAM_VALUE_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> "RecordNamespace"
    )
    val keyRegistryConfig = commonRegistryConfig +
      (SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY -> "topic.name")

    val valueRegistryConfig = commonRegistryConfig +
      (SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> "topic.record.name")
    (keyRegistryConfig, valueRegistryConfig)
  }



  def writeToKafka(kafkaFinalDf: DataFrame, extractedMap: Map[String, Any]): Unit = {
    //WRITING data to KAFKA ..based on the Arglist,flags.
    try {
      val kafkaConf = extractedMap.getOrElse("kafkaConf", "").asInstanceOf[Map[String, String]]
      val npspendKafkaKeyCols = extractedMap.getOrElse("npspendKafkaKeyCols", "").asInstanceOf[List[String]]
      val npspendKafkaValueCols = extractedMap.getOrElse("npspendKafkaValueCols", "").asInstanceOf[List[String]]
      KafkaProducer.dataPush(kafkaFinalDf, "npspend", kafkaConf, npspendKafkaKeyCols, npspendKafkaValueCols)
    } catch {
      case e: Exception => e.printStackTrace()

    }
  }

  /*
    // df to push the old data to kafka using date as string type

    val kafkaFinaloldlDf = groupedOldDf
      .withColumn("row_effective_date", lit(oldData_effective_date))
      .withColumn("row_expiry_date", lit(oldData_expiry_date))
      .withColumn("charge_amount", col("charge_amount").cast("string"))
      .withColumn("allowed_amount", col("allowed_amount").cast("string"))
      .withColumn("allowed_to_charge_ratio", col("allowed_to_charge_ratio").cast("string"))

 */

  /* df to push the data to kafka using date as string type
  val kafkaFinalDf = groupedDf1
     .withColumn("row_effective_date", lit(effDate))
     .withColumn("row_expiry_date", lit(expDate))
     .withColumn("charge_amount", col("charge_amount").cast("string"))
     .withColumn("allowed_amount", col("allowed_amount").cast("string"))
     .withColumn("allowed_to_charge_ratio", col("allowed_to_charge_ratio").cast("string"))
   */



}
