# Spark UDF Examples 
Simple examples of Spark SQL user-defined functions

### Load the sample data
<code>
hdfs dfs -put data/temperatures.json temperatures.json
</code>
<br/>
<code>
hdfs dfs -put data/inventory.json    inventory.json
</code>

### Build the Java and Scala examples
Under each example root (java-udf/, scala-udf/, ...):
<br/>
<code>
mvn package
</code>

### Run them
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

Hive UDF:
<br/>
<code>
spark-submit --jars target/hiveudf-0.0.1-jar-with-dependencies.jar  hive-udf-example.py
</code>

Scala UDAF From PySpark:
<br/>
<code>
spark-submit --jars target/scalaudaffrompython-0.0.1.jar --driver-class-path target/scalaudaffrompython-0.0.1.jar scala-udaf-from-python.py
</code>
