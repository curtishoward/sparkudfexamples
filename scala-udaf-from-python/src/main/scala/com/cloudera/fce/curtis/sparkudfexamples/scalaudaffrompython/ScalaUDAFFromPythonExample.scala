package com.cloudera.fce.curtis.sparkudfexamples.scalaudaffrompython

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext

object ScalaUDAFFromPythonExample {

  private class SumProductAggregateFunction extends UserDefinedAggregateFunction {
    def inputSchema: StructType =     
      new StructType().add("price", DoubleType).add("quantity", LongType)
    def bufferSchema: StructType =    
      new StructType().add("total", DoubleType)
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

  // This function is called from PySpark to register our UDAF
  def registerUdf(sqlCtx: SQLContext) {
    sqlCtx.udf.register("SUMPRODUCT", new SumProductAggregateFunction)
  }
}
