//Preparing df
val jsondf = spark.read.json("/FileStore/tables/test-1.json")
val jsondf2 = spark.read.json("/FileStore/tables/test-1.json")
val jsondf3 = spark.read.json("/FileStore/tables/test-1.json")
val jsondf4 = spark.read.json("/FileStore/tables/test-1.json")

//Take today date
val now = DateTimeFormatter.ofPattern("dd-MM-yyyy").format(LocalDateTime.now)

//Union first 3 df
val jsonu=jsondf.union(jsondf2).union(jsondf3)

//Add time column
val jsondf = jsonu.withColumn("Time",lit(now))

//Adding diferent time for last df (testing purpose)
val jsondf5 = jsondf4.withColumn("Time",lit("25-11-2020"))

//Uninon with last df
val jsonuionFinal = jsondf.union(jsondf5)

//Final -> Partitioning
jsonuionFinal.write.partitionBy("Time").mode("overwrite").parquet("/FileStore/partition/attempt5") //OK