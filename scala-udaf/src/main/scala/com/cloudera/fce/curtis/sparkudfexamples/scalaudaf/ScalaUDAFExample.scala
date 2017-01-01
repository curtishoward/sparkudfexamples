package com.cloudera.fce.curtis.sparkudfexamples.scalaudaf

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext

object ScalaUDAFExample {

  private class WeightedSumAggregateFunction extends UserDefinedAggregateFunction {

    def inputSchema: StructType =
      new StructType().add("price", DoubleType).add("quantity", LongType)
    def bufferSchema: StructType =
      new StructType().add("sumPrices", DoubleType)
    def dataType: DataType = DoubleType
    def deterministic: Boolean = true

    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, 0.0)
    }

    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val sum   = buffer.getDouble(0)
      val price = input.getDouble(0)
      val qty   = input.getLong(1)
      buffer.update(0, sum + (price * qty))
    }

    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getDouble(0) + buffer2.getDouble(0))
    }

    def evaluate(buffer: Row): Any = {
      buffer.getDouble(0)
    }
  }

  def main (args: Array[String]) {
    val conf       = new SparkConf().setAppName("Scala UDAF Example")
    val sc         = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val testDF = sqlContext.read.json("inventory.json")
    testDF.registerTempTable("inventory") 

    sqlContext.udf.register("SUMPRODUCT", new WeightedSumAggregateFunction)

    sqlContext.sql("SELECT Make, SUMPRODUCT(RetailValue,Stock) as InventoryValuePerMake FROM inventory GROUP BY Make").show()
  }
}
