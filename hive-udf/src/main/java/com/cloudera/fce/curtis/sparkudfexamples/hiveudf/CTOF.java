package com.cloudera.fce.curtis.sparkudfexamples.hiveudf;

import org.apache.hadoop.hive.ql.exec.UDF;
//import org.apache.hadoop.io.DoubleWritable;

public class CTOF extends UDF {
  public Double evaluate(Double degreesCelsius) {
    return ((degreesCelsius * 9.0 / 5.0) + 32.0);
  }
}
