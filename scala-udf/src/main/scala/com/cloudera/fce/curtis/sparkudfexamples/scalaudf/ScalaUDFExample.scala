package com.cloudera.fce.curtis.sparkudfexamples.scalaudf

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object ScalaUDFExample {
  def main(args: Array[String]) {
    val conf       = new SparkConf().setAppName("Scala UDF Example")
    val spark      = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate() 

    val ds = spark.read.json("temperatures.json")
    ds.createOrReplaceTempView("citytemps")

    // Register the UDF with our SparkSession 
    spark.udf.register("CTOF", (degreesCelcius: Double) => ((degreesCelcius * 9.0 / 5.0) + 32.0))

    spark.sql("SELECT city, CTOF(avgLow) AS avgLowF, CTOF(avgHigh) AS avgHighF FROM citytemps").show()
  }
}
