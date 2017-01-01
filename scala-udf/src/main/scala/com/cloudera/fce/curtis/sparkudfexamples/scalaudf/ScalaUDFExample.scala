package com.cloudera.fce.curtis.sparkudfexamples.scalaudf

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

object ScalaUDFExample {
  def main(args: Array[String]) {
    val conf       = new SparkConf().setAppName("Scala UDF Example")
    val sc         = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.json("temperatures.json")
    df.registerTempTable("citytemps")

    sqlContext.udf.register("CTOF", (celcius: Double) => ((celcius * 9.0 / 5.0) + 32.0))

    sqlContext.sql("SELECT city, CTOF(avgLow) AS avgLow, CTOF(avgHigh) AS avgHigh FROM citytemps").show()
  }
}
