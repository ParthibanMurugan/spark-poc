package com.spark.scala.practice.example.spark

import com.spark.scala.practice.example.common.SparkEnv
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object MapDynamicSchema extends SparkEnv {
  var toMap: (String) => (String, String) = (data) => {
    val arr = data.split(":")
    (arr(0), arr(1))
  }


  var rdd = spark.sparkContext.parallelize(Seq("country:IND,state:TN",
    "country:IND,state:KA,code:560066", "country:SIN,state:Woodlands,code:736688,phone:94990657"))

  val mapRdd = rdd.map(x => x.split(",").map(toMap.apply(_)).toMap)

  val cols = mapRdd.flatMap(x => x.keys).distinct().collect().toSeq

  val structType = StructType(cols.map(StructField(_, StringType, true)))

  val rowRDD = mapRdd.map(row => {
    val mapList = cols.map(x => (x, row.getOrElse(x, null))).toMap
    Row(mapList.values.toList: _*)
  })

  spark.createDataFrame(rowRDD, structType).show(false)

}
