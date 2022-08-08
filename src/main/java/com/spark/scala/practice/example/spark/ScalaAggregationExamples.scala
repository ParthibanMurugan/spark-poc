package com.spark.scala.practice.example.spark

import com.spark.scala.practice.example.common.SparkEnv
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._

object ScalaAggregationExamples extends SparkEnv{
  val filePath = getFilePath("Stores.csv")

  val df = readCsvFile(filePath)

  val renameDF = df.withColumnRenamed("`Store ID `","storeId")
    .withColumnRenamed("Store_Area","storeArea")
    .withColumnRenamed("Items_Available","itemsAvailable")
    .withColumnRenamed("Daily_Customer_Count","dailyCount")
    .withColumnRenamed("Store_Sales","storeSales")

  //renameDF.show(false)

  

  renameDF.groupBy("storeArea").agg(functions.collect_list(col("itemsAvailable"))).show(false)
  renameDF.groupBy("storeArea").pivot("`Store ID `").agg(sum("storeSales")).show(false)

}
