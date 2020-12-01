import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.{AnalysisException, Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode, lit, when}
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}



object Main {

  def main(args: Array[String]): Unit = {

    val now = DateTimeFormatter.ofPattern("dd-MM-yyyy").format(LocalDateTime.now)
    val spark = SparkSession.builder().appName("proba").master("local[*]").getOrCreate()


    val df = spark.read.option("multiline", true).json("./data/prod-data.json")
    val df1 = spark.read.option("multiline", true).json("./data/epim.json")
    val df2 = spark.read.option("multiline", true).json("./data/nestedMissing2.json")
    val df3 = spark.read.option("multiline", true).json("./data/nestedMissing3.json")


    val flatDf = flattenDataframe(df)
    /*val flatDf1 = flattenDataframe(df1)
    val flatDf2 = flattenDataframe(df2)
    val flatDf3 = flattenDataframe(df3)*/

    flatDf.printSchema()

    flatDf.show()

    /*flatDf1.printSchema()
    flatDf1.show()

    flatDf2.printSchema()
    flatDf2.show()

    flatDf3.printSchema()
    flatDf3.show()*/


    /*val completeDf = flatDf1.join(flatDf2,col("data_id"),"outer").join(flatDf3,col("data_id"),"outer")

    completeDf.show()*/

    // Partitioning
    /*df.createOrReplaceTempView("table")

    val df1 = spark.sql("SELECT *, gfcid % 2000 as part FROM table")

    df1.write.partitionBy("part").csv("./output/")
*/
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