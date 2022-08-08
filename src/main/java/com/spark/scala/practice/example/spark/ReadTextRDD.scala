package com.spark.scala.practice.example.spark

import com.spark.scala.practice.example.common.SparkEnv
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.ScalaReflection.Schema
import org.apache.spark.sql.types.{HIVE_TYPE_STRING, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, RowFactory}

object ReadTextRDD extends SparkEnv{
  val filePath = "src/main/resources/spark/employee.txt"
  case class Employee(name : String,salary : String,sex : String,age :String)
  def readAsCSVDF(path : String) : DataFrame={
    val options = Map("delimiter"->",",
    "header"->"true",
    "inferSchema"->"true")

    spark.read
      .format("csv")
      .options(options)
      .load(path)
  }

  def readAsRDD(path : String) : DataFrame={
    val rddData = spark.sparkContext.textFile(path)

    val header = rddData.first()

    println(header)

    val headerList = header.split(",").toList

    val list = headerList.map(row=>{
      StructField.apply(row,StringType)
    }).toList

    val structType1 = StructType.apply(list)

    val data = rddData.filter(row=> !row.matches(header))
      .map(row=>{
        val array = row.split(",").toList
        RowFactory.create(array:_*)
      })

    //data.map(row=>row.schema.

    val structType = ScalaReflection.schemaFor[Employee].dataType.asInstanceOf[StructType]
    val df = spark.createDataFrame(data,structType)

    (df)
  }

  val data = readAsRDD(filePath)

  data.show(false)

}
