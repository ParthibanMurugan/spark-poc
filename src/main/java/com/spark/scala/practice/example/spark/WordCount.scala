package com.spark.scala.practice.example.spark

import com.spark.scala.practice.example.common.SparkEnv
import com.spark.scala.practice.example.spark.WordCount.data
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Row, functions}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

object WordCount extends SparkEnv{
  import spark.implicits._
  var data = spark.sparkContext.textFile("src/main/resources/spark/content.txt")
 //Group BY Key
  data.filter(line=>line.length > 0)
    .flatMap(line=>line.split(" "))
    .map(word=>(word,1))
    .groupByKey()
    .map(data=>(data._1,data._2.toList.sum))
    .foreach(data=>println(s"key :${data._1} value:${data._2}"))

  //Reduce By Key
  data.filter(line=>line.length > 0)
    .flatMap(line=>line.split(" "))
    .map(word=>(word,1))
    .reduceByKey((cur,prev)=>cur+prev)
    .foreach(data=>println(s"key :${data._1} value:${data._2}"))

  //Convert the rdd to Dataframe
  val structType = new StructType()
    .add(StructField("key",StringType,false))
    .add(StructField("value",ArrayType(IntegerType,true),true))

  val rdd = data.filter(line=>line.length > 0)
    .flatMap(line=>line.split(" "))
    .map(word=>(word,1))
    .groupByKey()
    .map(data=>Row(data._1,data._2.toList))
   // .toDF("key","value")

  //rdd.show()
  //rdd.printSchema()

  val df = spark.createDataFrame(rdd,structType)

  df.withColumn("explode_col",functions.explode(col("value")))
    .show(false)

  //Use Spar implicits to convert RDD to Dataframe
  import spark.implicits._
  data.filter(line=>line.length > 0)
    .flatMap(line=>line.split(" "))
    .map(word=>(word,1))
    .groupByKey()
    .map(data=>(data._1,data._2.toList))
    .toDF("key","value")
    .show(false)

}
