package com.cloudera.fce.curtis.sparkudfexamples.javaudf;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class JavaUDFExample {
  public static void main(String[] args) {
    SparkConf conf        = new SparkConf().setAppName("Java UDF Example");
    SparkSession spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate(); 
 
    Dataset<Row> ds = spark.read().json("temperatures.json");
    ds.createOrReplaceTempView("citytemps");
   
    // Register the UDF with our SparkSession 
    spark.udf().register("CTOF", new UDF1<Double, Double>() {
      @Override
      public Double call(Double degreesCelcius) {
        return ((degreesCelcius * 9.0 / 5.0) + 32.0);
      }
    }, DataTypes.DoubleType);
    
    spark.sql("SELECT city, CTOF(avgLow) AS avgLowF, CTOF(avgHigh) AS avgHighF FROM citytemps").show();
  }
}
