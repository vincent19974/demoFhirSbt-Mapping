package com.p360.npspend.claims.analytics

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, struct, _}
import za.co.absa.abris.avro.format.SparkAvroConversions
import za.co.absa.abris.avro.functions.to_confluent_avro
import za.co.absa.abris.avro.read.confluent.SchemaManager



object KafkaJsonProducer extends Logging {

    def dataPush(kafkaFinalDf: DataFrame, subjectName: String, kafkaConf: Map[String, String]
                ): Unit = {
      logInfo("Performing Kafka datapush for :" + kafkaFinalDf)


      //Performing dataPush to Kafka using ABRIS Package
      kafkaFinalDf
        .select(col("key"), col("value"))
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


    def writeToKafka(kafkaFinalDf: DataFrame, extractedMap: Map[String, Any]): Unit = {
      //WRITING data to KAFKA ..based on the Arglist,flags.
      try {
        val kafkaConf = extractedMap.getOrElse("kafkaConf", "").asInstanceOf[Map[String, String]]
        KafkaJsonProducer.dataPush(kafkaFinalDf, "npspend", kafkaConf)
      } catch {
        case e: Exception => e.printStackTrace()

      }
    }




}
