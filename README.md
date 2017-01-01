# Spark UDF Examples 
Simple examples of Spark SQL user-defined functions

### Save the input data to HDFS:
<br/>
<code>
hdfs dfs -put data/temperatures.json temperatures.json
</code>
<br/>
<code>
hdfs dfs -put data/inventory.json    inventory.json
</code>

### Build the Java and Scala examples
Under each example root (java-udf/, scala-udf/, scala-udaf/):
<code>
mvn package
</code>

### Run the examples (locally)
Python UDF:
<br/>
<code>
spark-submit --master local python-udf-example.py
</code>

Scala UDF:
<br/>
<code>
spark-submit --class com.cloudera.fce.curtis.sparkudfexamples.scalaudf.ScalaUDFExample --master local target/scalaudf-0.0.1-jar-with-dependencies.jar
</code>

Java UDF:
<br/>
<code>
spark-submit --class com.cloudera.fce.curtis.sparkudfexamples.javaudf.JavaUDFExample  --master local target/javaudf-0.0.1-jar-with-dependencies.jar
</code>

Scala UDAF:
<br/>
<code>
spark-submit --class com.cloudera.fce.curtis.sparkudfexamples.scalaudaf.ScalaUDAFExample --master local target/scalaudaf-0.0.1-jar-with-dependencies.jar
</code>

