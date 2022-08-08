package com.spark.scala.practice.example.certificate.prep

import com.spark.scala.practice.example.common.SparkEnv
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{Encoders, functions}
//patientID,name,dateOfBirth,lastVisitDate
case class Patient(patientID : String,name : String,dateOfBirth :String,lastVisitDate : String)
object PrepScala extends SparkEnv{

  //val encoder = Encoders.kryo()
  val encoder = org.apache.spark.sql.Encoders.product[Patient]

  var data = spark.read.format("csv").option("header","true").option("delimiter",",")
    .load("src/main/resources/spark/patient.csv")
  val data_1 = data.as(encoder)

 // val expression =


  data_1.filter("name = 'Ali' or name = 'Kumar'").show(false)

  data_1.filter(functions.col("name") === "Kumar" || functions.col("name")==="Ali").show(false)

  val expression = data_1.col("name").equalTo("Kumar").or(data_1.col("name").equalTo("Ali"))
  //data_1.col("name").e
  data_1.filter(expression).show(false)

  val simpleColors = Seq("black", "white", "red", "green", "blue")
  val selectedColumns = simpleColors.map(color => {
    col("Description").contains(color.toUpperCase).alias(s"is_$color")
  }):+expr("*")

  println(selectedColumns)

  //data_1.printSchema()
  //data_1.show(false)
}
