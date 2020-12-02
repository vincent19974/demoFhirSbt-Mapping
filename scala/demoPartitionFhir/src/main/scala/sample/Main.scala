import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.catalyst.ScalaReflection.universe.typeOf
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, Row, SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions.{col, explode, lit, when}
import org.apache.spark.sql.types.{ArrayType, StructType}

import scala.collection.mutable.ArrayBuffer



object Main {

  def main(args: Array[String]): Unit = {

    //val now = DateTimeFormatter.ofPattern("dd-MM-yyyy").format(LocalDateTime.now)
    val spark = SparkSession.builder().appName("proba").master("local[*]").getOrCreate()


    val df = spark.read.option("multiline", true).json("./data/input/epim-not-working.json")
    val flatDf = flattenDataframe(df)

//     val colList: Array[Column] = Array(
//      col("id").as("EPIM_Identifier")
//      , col("entry_resource_identifier_value").as("NPI")
//      , col("entry_resource_active").as("Provider_Active_Indicator")
//      , col("entry_resource_identifier_value").as("Identifier")
//      , col("entry_resource_identifier_use").as("Identifier_Use_Code")
//      , col("entry_resource_identifier_type_coding_code").as("Identifier_Type")
//      , col("entry_resource_identifier_period_start").as("Period_Start_Date")
//      , col("entry_resource_identifier_period_end").as("Period_End_Date")
//      , col("entry_resource_identifier_assigner_display").as("Assigner_Organization_Name")
//      , col("entry_resource_identifier_characteristic_value").as("State_Code")
//    )

    val dfNew = flatDf.select(col("id").as("EPIM_Identifier")
      , col("entry_resource_identifier_value").as("NPI")
      , col("entry_resource_active").as("Provider_Active_Indicator")
      , col("entry_resource_identifier_value").as("Identifier")
      , col("entry_resource_identifier_use").as("Identifier_Use_Code")
      , col("entry_resource_identifier_type_coding_code").as("Identifier_Type")
      , col("entry_resource_identifier_period_start").as("Period_Start_Date")
      , col("entry_resource_identifier_period_end").as("Period_End_Date")
      , col("entry_resource_identifier_assigner_display").as("Assigner_Organization_Name")
      , col("entry_resource_identifier_characteristic_value").as("State_Code"))


    partition(dfNew, 100)
  }

  def partition(df: DataFrame,numPartitions: Int) = {
    var dff = df.withColumn("part", functions.hash(col("EPIM_Identifier")) % numPartitions)
    dff.groupBy("EPIM_Identifier").mean("part").show()
    dff.write.partitionBy("part").mode("overwrite").json("./data/output")
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