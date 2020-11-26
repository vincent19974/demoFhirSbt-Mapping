Input:

Input is 4 data frames union in one master dataframe.

Output:

Output is structured files in our file system.

Flow

First we import 4 json files and union them in a single one.

    val df4 = spark.read.option("multiline", true).json("./Entry.json")
    val df1 = spark.read.option("multiline", true).json("./Entry.json")
    val df2 = spark.read.option("multiline", true).json("./Entry.json")
    val df3 = spark.read.option("multiline", true).json("./Entry.json")

    var df = df4.union(df1).union(df2).union(df3)


    var df = df4.union(df1).union(df2).union(df3)

Then we ask if our data frame contains a time column. If the answer is yes we pass through him and where is null we put today date otherwise we live what was there. 

  try {
    df = df.withColumn("time", when(df("time").isNull, now).otherwise(df("time")))
    df.show()
  }catch {
    case e: AnalysisException => {
      df = df.withColumn("time", lit(now))
    }
  }

Finally in the end we save that data frame on some path in partitions time and left or right.

finally {
    df.write.partitionBy("time", "leftOrRight").mode("overwrite").json("./output/")
  }

