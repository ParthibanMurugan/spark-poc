package com.spark.scala.practice.example.spark

import com.spark.scala.practice.example.common.SparkEnv

object SparkBasic extends SparkEnv{
  val seqNumbers = Stream.range(1,10).toList
  val data = spark.sparkContext.parallelize(seqNumbers,3)
  //Use glom method to create the pair RDD with partitions, and can apply operation on each partitions
  println(data.glom().collect().toList.toList)

  data.map(num=>{
    val result = num%2 match{
      case 0 => "Even"
      case 1 => "Odd"
    }
    (result,num)
  }).groupByKey().collect().foreach(x=>println(x._1+" Values"+x._2.toList))

  val a = spark.sparkContext.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
  val aRdd = a.keyBy(_.length)

  //a.keyBy(row=>(row.length,row))
  val c = spark.sparkContext.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
  val cRdd = c.keyBy(_.length)

  aRdd.rightOuterJoin(cRdd)



}
