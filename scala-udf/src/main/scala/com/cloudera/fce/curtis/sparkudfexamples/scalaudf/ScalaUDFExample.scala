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

package com.cloudera.fce.curtis.sparkudfexamples.scalaudf

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._

object ScalaUDFSample {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Scala UDF Example")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val testDF = sqlContext.read.json("udfTestInput.json")
    testDF.registerTempTable("testDF")

    // sqlContext.registerFunction(
    sqlContext.udf.register("CURTIS", (f: Double) => ((f*9.0/5.0)+32.0))
    sqlContext.sql("SELECT CURTIS(numVal) AS Fahrenheit FROM testDF").show()
    sc.stop()
  }
}
