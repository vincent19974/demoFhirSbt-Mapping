package sample

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import breeze.numerics.constants.e
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.internal.Constants
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, lit, when}

import scala.reflect.runtime.universe.typeOf
import scala.util.control.Exception

object Main {



  def main(args: Array[String]): Unit = {

    val now = DateTimeFormatter.ofPattern("dd-MM-yyyy").format(LocalDateTime.now)
    val spark = SparkSession.builder().appName("proba").master("local[*]").getOrCreate()


    val df4 = spark.read.option("multiline", true).json("./Entry.json")
    val df1 = spark.read.option("multiline", true).json("./Entry.json")
    val df2 = spark.read.option("multiline", true).json("./Entry.json")
    val df3 = spark.read.option("multiline", true).json("./Entry.json")

    var df = df4.union(df1).union(df2).union(df3)
    df = df.repartition(7, col("id"))
  try {
    df = df.withColumn("time", when(df("time").isNull, now).otherwise(df("time")))
    df.show()
  }catch {
    case e: AnalysisException => {
      df = df.withColumn("time", lit(now))
    }
  }finally {
  
    df.write.partitionBy("time", "leftOrRight").mode("overwrite").json("./output/")
  }

  }


}
