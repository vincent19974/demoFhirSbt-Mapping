import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, Row, SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions.{abs, col, explode, lit, when}
import org.apache.spark.sql.types.{ArrayType, StructType}

import scala.util.control.Breaks.{break, breakable}



object Main {

  def main(args: Array[String]): Unit = {


    val now = DateTimeFormatter.ofPattern("dd-MM-yyyy").format(LocalDateTime.now)
    val spark = SparkSession.builder().appName("proba").master("local[*]").getOrCreate()

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val df1 = spark.read.option("multiline", true).json("./data/prod-data.json")
    val df2 = spark.read.option("multiline", true).json("./data/epim-not-working.json")


    var flatDf1 = flattenDataframe(df1)
    var flatDf2 = flattenDataframe(df2)

    flatDf1.printSchema()

    flatDf1.show()
    //flatDf1.createOrReplaceTempView("table1")
    //spark.sql("SELECT distinct id FROM table1").show()


    flatDf2.printSchema()

    flatDf2.show()
    //flatDf2.createOrReplaceTempView("table2")
    //spark.sql("SELECT distinct id FROM table2").show()

    flatDf1 = renameColumns(flatDf1, "A")
    flatDf2 = renameColumns(flatDf2, "B")

    //flatDf1.show()
    //flatDf2.show()

    val totalDf = flatDf1.join(flatDf2,Seq("id"),"outer")
    totalDf.printSchema()
    //totalDf.show()

    print(" First " + flatDf1.count() + " Second: " + flatDf2.count() + " Third: " + totalDf.count() + "\n")

    /*val completeDf = flatDf1.join(flatDf2,col("data_id"),"outer").join(flatDf3,col("data_id"),"outer")

    completeDf.show()*/


    val mappedDf = mapDf(totalDf)

    mappedDf.show()

    partition(mappedDf, 100)


  }



  def flattenDataframe(df: DataFrame): DataFrame = {

    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)

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

  def renameColumns(df: DataFrame, tableName : String): DataFrame = {

    var tempDf = df
    val fields = df.schema.fields
    for(i <- 0 to fields.length-1){
      breakable {
        val field = fields(i)
        val fieldName = field.name
        if (fieldName == "id") break
        tempDf = tempDf.withColumnRenamed(fieldName, fieldName + tableName)
      }
    }
    tempDf
  }

  def mapDf(df: DataFrame): DataFrame = {

    var tempDf = df

    tempDf = tempDf.select(col("id").as("EPIM_Identifier"),

      when(col("entry_resource_identifier_type_coding_codeA").equalTo("NPI"), col("entry_resource_identifier_valueA")).
        when(col("entry_resource_identifier_type_coding_codeB").equalTo("NPI"), col("entry_resource_identifier_valueB"))
        .otherwise(null).as("NPI"),

      when(col("entry_resource_activeA").isNotNull, col("entry_resource_activeA")).
        when(col("entry_resource_activeB").isNotNull, col("entry_resource_activeB"))
        .otherwise(null).as("Provider_Active_Indicator"),

      when(col("entry_resource_identifier_valueA").isNotNull, col("entry_resource_identifier_valueA")).
        when(col("entry_resource_identifier_valueB").isNotNull, col("entry_resource_identifier_valueB"))
        .otherwise(null).as("Identifier"),

      when(col("entry_resource_identifier_useA").isNotNull, col("entry_resource_identifier_useA")).
        when(col("entry_resource_identifier_useB").isNotNull, col("entry_resource_identifier_useB"))
        .otherwise(null).as("Identifier_Use_Code"),

      when(col("entry_resource_identifier_type_coding_codeA").isNotNull, col("entry_resource_identifier_type_coding_codeA")).
        when(col("entry_resource_identifier_type_coding_codeB").isNotNull, col("entry_resource_identifier_type_coding_codeB"))
        .otherwise(null).as("Identifier_Type"),

      when(col("entry_resource_identifier_period_startA").isNotNull, col("entry_resource_identifier_period_startA")).
        when(col("entry_resource_identifier_period_startA").isNotNull, col("entry_resource_identifier_period_startA"))
        .otherwise(null).as("Period_Start_Date"),

      when(col("entry_resource_identifier_period_endA").isNotNull, col("entry_resource_identifier_period_endA")).
        when(col("entry_resource_identifier_period_endB").isNotNull, col("entry_resource_identifier_period_endB"))
        .otherwise(null).as("Period_End_Date"),

      when(col("entry_resource_identifier_period_activeA").isNotNull, col("entry_resource_identifier_period_activeA")).
        when(col("entry_resource_identifier_period_activeB").isNotNull, col("entry_resource_identifier_period_activeB"))
        .otherwise(null).as("Period_Active_Indicator"),

      when(col("entry_resource_identifier_assigner_displayA").isNotNull, col("entry_resource_identifier_assigner_displayA")).
        when(col("entry_resource_identifier_assigner_displayB").isNotNull, col("entry_resource_identifier_assigner_displayB"))
        .otherwise(null).as("Assigner_Organisation_Name"),

      when(col("entry_resource_extension_valueMetadata_sourceSystemCdA").isNotNull, col("entry_resource_extension_valueMetadata_sourceSystemCdA")).
        when(col("entry_resource_extension_valueMetadata_sourceSystemCdB").isNotNull, col("entry_resource_extension_valueMetadata_sourceSystemCdB"))
        .otherwise(null).as("Source_System_Code")

    )

    tempDf
  }

  def partition(df: DataFrame,numPartitions: Int) = {
    var dff = df.withColumn("part", abs(functions.hash(col("EPIM_Identifier")) % numPartitions))
    dff.write.partitionBy("part").mode("overwrite").parquet("./data/output")
  }
}