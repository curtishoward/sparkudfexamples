# Spark UDF Examples 
Simple examples of Spark SQL user-defined functions

First, save the input data to HDFS:
<br/>
<code>
hdfs dfs -put data/temperatures.json temperatures.json
</code>
<br/>
<code>
hdfs dfs -put data/inventory.json    inventory.json
</code>

To run the Python UDF example locally:
<br/>
<code>
spark-submit --master local python-udf-example.py
</code>

To run the Scala UDF example locally:
<br/>
<code>
spark-submit --class com.cloudera.fce.curtis.sparkudfexamples.scalaudf.ScalaUDFExample --master local target/scalaudf-0.0.1-jar-with-dependencies.jar
</code>

To run the Java UDF example locally:
<br/>
<code>
spark-submit --class com.cloudera.fce.curtis.sparkudfexamples.javaudf.JavaUDFExample  --master local target/javaudf-0.0.1-jar-with-dependencies.jar
</code>

To run the Scala UDAF example locally:
<br/>
<code>
spark-submit --class com.cloudera.fce.curtis.sparkudfexamples.scalaudaf.ScalaUDAFExample --master local target/scalaudaf-0.0.1-jar-with-dependencies.jar
</code>

