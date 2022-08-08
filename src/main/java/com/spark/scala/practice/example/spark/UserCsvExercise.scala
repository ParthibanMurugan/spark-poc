package com.spark.scala.practice.example.spark

import com.spark.scala.practice.example.common.SparkEnv

object UserCsvExercise extends SparkEnv{
  val sc = spark.sparkContext
  val userFilePath = "src/main/resources/spark/user.csv"

  val data = sc.textFile(userFilePath)
    .map(_.trim)
    .map(_.replace("-",""))

  val header = data.first()
  val headerList = header.split(",").toList
  //headerList.z
  data.filter(row=> !row.matches(header))
    .map(row=>{
      headerList.zip(row.split(",")).toMap
      //headerList.z
    }).collect().foreach(println(_))
}
