package com.spark.scala.practice.example.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, RowFactory, SparkSession, functions}

class SparkDFExercise(spark : SparkSession) {

  private def readCsvFile(path: String, options: Map[String, String], structType: StructType): DataFrame = {

    spark.read
      .format("csv")
      .options(options)
      .schema(structType)
      .load(path)
  }

  private def readAsRDD(path: String): RDD[String] = {
    spark.sparkContext
      .textFile(path)
      .filter(_.length > 0)
      .map(_.trim)
  }

  private def getStructTypeFromList(list: List[String]): List[StructField] = {
    list.map(col => {
      StructField.apply(col, StringType)
    })
  }

  private def convertToDF(rdd: RDD[String], header: List[String]): DataFrame = {
    spark.createDataFrame(rdd.map(line => RowFactory.create(line.split(","): _*)), StructType.apply(getStructTypeFromList(header)))
  }

  def joinRDDExample(parameters: Map[String, String]): DataFrame = {
    val techRDD = readAsRDD(parameters.getOrElse("techFile", "src/main/resources/spark/technology.csv"))
    val salRDD = readAsRDD(parameters.getOrElse("salFile", "src/main/resources/spark/salary.csv"))

    val techHeader = techRDD.first()
    val salHeader = salRDD.first()

    val joinCol = techHeader.split(",").intersect(salHeader.split(","))

    val techDF = convertToDF(techRDD, techHeader.split(",").toList)
    val salDF = convertToDF(salRDD, salHeader.split(",").toList)

    techDF.join(salDF, joinCol, "inner")
  }

  def loadPatientInfo(path: String): DataFrame = {
    val readOptions = Map("delimiter" -> ",",
      "header" -> "true",
      "dateFormat" -> "yyyy-MM-dd")

    val structType = StructType(Array(StructField.apply("patientID", IntegerType),
      StructField.apply("name", StringType),
      StructField.apply("dateOfBirth", DateType),
      StructField("lastVisitDate", DateType)
    ))
    val data = readCsvFile(path, readOptions, structType)

    //data.na.replace()


    ///functions.mo
    data.withColumn("age",functions.floor(functions.months_between(functions.current_date(),data.col("dateOfBirth"),true)./(12)))
      .withColumn("year",functions.date_format(functions.col("dateOfBirth"),"yyyy"))
      .withColumn("daysVisited",functions.datediff(functions.current_date(),functions.col("lastVisitDate")))
   // (data)
  }
}

