package com.cloudera.fce.curtis.sparkudfexamples.javaudf;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class JavaUDFExample {
  public static void main(String[] args) {
    SparkConf conf        = new SparkConf().setAppName("Java UDF Example");
    JavaSparkContext sc   = new JavaSparkContext(conf);
    SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

    DataFrame df = sqlContext.read().json("temperatures.json");
    df.registerTempTable("citytemps");
   
    // Register the UDF with our SQLContext
    sqlContext.udf().register("CTOF", new UDF1<Double, Double>() {
      @Override
      public Double call(Double celcius) {
        return ((celcius * 9.0 / 5.0) + 32.0);
      }
    }, DataTypes.DoubleType);
    
    sqlContext.sql("SELECT city, CTOF(avgLow) AS avgLow, CTOF(avgHigh) AS avgHigh FROM citytemps").show();
  }
}
