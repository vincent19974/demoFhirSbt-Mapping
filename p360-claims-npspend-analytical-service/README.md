# p360-analytical-network-measures
This Repo Is for Analytical Network Measures

## Spark-Job command:
Spark logger set to "ERROR"

spark-submit --master yarn \
--class com.p360.network.measures.MainObject \
--packages org.apache.commons:commons-email:1.5,org.apache.spark:spark-avro_2.11:2.4.4 \
s3://p360-nonprod-network-measures/Stage/p360-network-measures.jar \
s3://p360-nonprod-network-measures/Stage/networkMeasuresConf.json RX



Note: arg1 = s3://path/to/conf.json
      arg2 = coma delemeted metrics ex: RX,LAB
----------------------------

## Configuration file needed:
network_measures_conf.json
https://github.optum.com/EnterpriseProviderPlatform/p360-analytical-network-measures/blob/development_prabhuja/src/main/resources/networkMeasuresConf.json

networkMeasuresCols.json
https://github.optum.com/EnterpriseProviderPlatform/p360-analytical-network-measures/blob/development_prabhuja/src/main/resources/networkMeasuresCols.json

----------------------------


Sample Submit command:

spark-submit \
--class com.p360.network.measures.MainObject \
--conf spark.executor.memory=5g \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=1 \
--conf spark.dynamicAllocation.maxExecutors=10 \
--packages org.apache.commons:commons-email:1.5,org.apache.spark:spark-avro_2.11:2.4.4,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4,io.confluent:kafka-avro-serializer:5.1.0,za.co.absa:abris_2.11:3.2.2,com.fasterxml.jackson.module:jackson-module-scala_2.11:2.11.2 \
--repositories http://packages.confluent.io/maven \
s3://p360-poc-generic/spark-jar/network-measures/p360-network-measures.jar \
s3://p360-poc-generic/hql/network-measures/networkMeasuresConf.json RX,SURG,LAB
