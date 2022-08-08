package com.spark.scala.practice.example.spark.udf

import com.spark.scala.practice.example.common.SparkEnv
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StringType

object UDFSample extends SparkEnv {

  def concatStr(stra: String, strb: String): String = {
    stra.concat(" ").concat(strb)
  }

  import spark.implicits._

  spark.udf.register("catUDF", concatStr(_: String, _: String))
  val cUDF = functions.udf(concatStr(_: String, _: String), StringType)
  val cols = Seq("fname", "lname", "gender");

  var nullFuncCols = cols.toStream
    .map(c => {
      functions.sum(functions.when((col(c).isNull || col(c).isNaN), lit(1)).otherwise(lit(0))) as (c)
    }).toSeq

  val colList = nullFuncCols :+ functions.count(lit(1)).as("count")

  val data = Seq(("Parthiban", "Murugan", "Male"), ("Preethi", "Parthiban", "Female")
    , ("Thaswika", "Vahini", "Female")).toDF(cols: _*)

  data.withColumn("fullName", cUDF(col("fname"), col("lname")))
    //.withColumn("")
    .show(false)

  data.select(colList: _*).show(false)


}
