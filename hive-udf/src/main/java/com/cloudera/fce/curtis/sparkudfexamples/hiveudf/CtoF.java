package com.cloudera.fce.curtis.sparkudfexamples.hiveudf;

import org.apache.hadoop.hive.ql.exec.UDF;
//import org.apache.hadoop.io.DoubleWritable;

public class CtoF extends UDF {
  public Double evaluate(Double celcius) {
    return ((celcius * 9.0 / 5.0) + 32.0);
  }
}
