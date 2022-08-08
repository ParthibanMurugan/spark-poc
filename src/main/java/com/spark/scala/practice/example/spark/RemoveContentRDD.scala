package com.spark.scala.practice.example.spark

import com.spark.scala.practice.example.common.SparkEnv
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.dsl.expressions.booleanToLiteral

object RemoveContentRDD extends SparkEnv{
  val contentPath = "src/main/resources/spark/content.txt"
  val filterPath = "src/main/resources/spark/remove.txt"

  def readAndCleanseData(path : String) : RDD[String]={
    spark.sparkContext
      .textFile(path)
      .filter(row=> !row.isEmpty)
      .filter(row=> row.length>0)
  }

  val contentRDD = readAndCleanseData(contentPath)
  val filterRDD = readAndCleanseData(filterPath)
    .flatMap(line=>line.split(","))
    .map(_.trim)
    .map(_.toUpperCase())
    .collect()

  val filterValue = spark.sparkContext.broadcast(filterRDD)

 /* contentRDD.flatMap(line=>line.split(" "))
    .filter(word=> !filterValue.value.contains(word.toUpperCase()))
    .foreach(println(_))

  */

  contentRDD.flatMap(line=>line.split(" "))
    .map(_.trim)
    .filter{
    case (word) => !filterValue.value.contains(word.toUpperCase())
  }.map((_,1))
    .reduceByKey(_+_)
    .collect()
    .foreach((k)=>println("key :"+k._1+" count:"+k._2))

}
