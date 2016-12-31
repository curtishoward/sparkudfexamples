/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package com.cloudera.fce.curtis.sparkudfexamples.javaudfexample;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class JavaUDFExample {
  public static void main(String[] args) {
    JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Java UDF Example"));
    SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
    //DataFrame df = sqlContext.read().json("/user/hdfs/udfTestInput.json");
    DataFrame df = sqlContext.read().json("/user/hdfs/doubleudfdata.json");
    df.registerTempTable("testDF");
    
    sqlContext.udf().register("CURTIS", new UDF1<Double, Double>() {
      @Override
      public Double call(Double c) {
        return ((c * 9.0 / 5.0) + 32.0);
        //return c;
      }
    }, DataTypes.DoubleType);
    
    sqlContext.sql("SELECT CURTIS(numVal) AS Fahrenheit FROM testDF").show();
    //sqlContext.sql("SELECT * FROM testDF").show();

  }
}
