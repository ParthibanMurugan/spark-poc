package com.spark.scala.practice.example.spark

import com.spark.scala.practice.example.common.SparkEnv
import org.apache.spark.sql.functions

object TestImplementation extends SparkEnv{
/*Write a Spark program, which will join the data based on first and last name and save the joined results in following format, first Last.technology.salary*/
  //Solution
  val parameters = Map("techFile"->"src/main/resources/spark/technology.csv",
    "salFile"->"src/main/resources/spark/salary.csv")

  val sparkDFExercise = new SparkDFExercise(spark)

  val data = sparkDFExercise.joinRDDExample(parameters)
 // data.show(false)
  /*
  Problem Scenario 54 : You have been given below code snippet. val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle")) val b = a.map(x => (x.length, x)) operation1
Write a correct code snippet for operationl which will produce desired output, shown below.
Array[(lnt, String)] = Array((4,lion), (7,panther), (3,dogcat), (5,tigereagle))
   */
  def foldString(prev : String,curr : String) : String={
    prev+curr
  }
  val stringList = List("dog", "tiger", "lion", "cat", "panther", "eagle")
  spark.sparkContext
    .parallelize(stringList)
    .keyBy(_.length)
    //.reduceByKey((a,b)=>a+b)
    //.fold((0,""))((a,b)=>a.)
    .foldByKey("")(foldString)
    //.collect()
    //.foreach(println(_))

  val index = spark.sparkContext
    .parallelize(Range.apply(1,10),3)
    .glom().partitions.toList.map(partition=>partition.index+1)

    //println(index)
  val animalList = List("dog", "cat", "owl", "gnu", "ant")
  val animalRDD = spark.sparkContext.parallelize(animalList, 2)
  val indexRDD = spark.sparkContext.parallelize(Range.apply(1,animalList.size+1),2)

 animalRDD.zip(indexRDD)
 indexRDD.zip(animalRDD).sortByKey(false)

/*
Problem Scenario 63 : You have been given below code snippet. val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2) val b = a.map(x => (x.length, x)) operation1
Write a correct code snippet for operationl which will produce desired output, shown below.
 */
  val a = spark.sparkContext.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)

  a.keyBy(_.length)
    .foldByKey("")((prev,curr)=>prev+curr)

  val patientData = sparkDFExercise.loadPatientInfo("src/main/resources/spark/patient.csv")

  patientData.printSchema()

  patientData
    .where("lastVisitDate between date_format('2012-09-15','yyyy-MM-dd') and current_date")
    //.filter("date_format(dateOfBirth,'yyyy')")
    .show(false)


}
