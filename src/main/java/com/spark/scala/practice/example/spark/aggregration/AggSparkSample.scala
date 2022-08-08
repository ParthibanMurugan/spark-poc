package com.spark.scala.practice.example.spark.aggregration

import com.spark.scala.practice.example.common.SparkEnv
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, count, lit, sum}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object AggSparkSample extends SparkEnv{
  /*
  id: integer (nullable = true)
 |-- work_year: integer (nullable = true)
 |-- experience_level: string (nullable = true)
 |-- employment_type: string (nullable = true)
 |-- job_title: string (nullable = true)
 |-- salary: integer (nullable = true)
 |-- salary_currency: string (nullable = true)
 |-- salary_in_usd: integer (nullable = true)
 |-- employee_residence: string (nullable = true)
 |-- remote_ratio: integer (nullable = true)
 |-- company_location: string (nullable = true)
 |-- company_size: string (nullable = true)
   */
  def getStructType() : StructType={
    new StructType()
      .add(StructField("id",IntegerType,false))
      .add(StructField("workyear",IntegerType,false))
      .add(StructField("expLevel",StringType,false))
      .add(StructField("empType",StringType,false))
      .add(StructField("jobTitle",StringType,false))
      .add(StructField("salary",DoubleType,false))
      .add(StructField("salaryCurrency",StringType,false))
      .add(StructField("salaryInUSD",DoubleType,false))
      .add(StructField("empResidence",StringType,false))
      .add(StructField("remoteRatio",IntegerType,false))
      .add(StructField("companyLocation",StringType,false))
      .add(StructField("companySize",StringType,false))

  }
  val filePath = "/home/parthiban/Work/datasets/ds_salaries.csv"

  val options = Map("delimiter"->",",
  "header"->"true",
  "mode"->"FAILFAST",
  "inferSchema"->"true")

  val df = readCsvFile(filePath,options,getStructType())

  df.show(false)
  df.printSchema()


  val windowSpec = Window.partitionBy("empType").orderBy(col("salaryInUSD").desc)

  df.withColumn("rn",functions.row_number().over(windowSpec))
    .where("rn <=3")
   // .show(false)
 // df.groupBy("empType").p
  val country = Seq("IN","DE")

  df.cube("empType","companyLocation")
    .agg(functions.grouping_id().as("level"),sum("salaryInUSD").as("salary"))
    .where("level = 1")
    .show(false)

  df.rollup("empType","companyLocation")
    .agg(functions.grouping_id().as("level"),sum("salaryInUSD").as("salary"))
    .where("level = 1")
    .show(false)



  df.groupBy("empType","jobTitle")
    .pivot("companyLocation",country)
    .sum("salaryInUSD")
    //.agg(sum("salaryInUSD").as("sal"),count(lit(1)).as("count"))
    .na
    .drop("all")
    .show(false)


  df.groupBy("empType","jobTitle")
    .agg(Map("salaryInUSD"->"sum","id"->"count"))
    .show(false)


}
