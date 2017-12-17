# Spark UDF Examples 
Simple examples of Spark SQL user-defined functions

### Load the sample data
```
hdfs dfs -put data/temperatures.json temperatures.json
hdfs dfs -put data/inventory.json    inventory.json
```

### Build the Java and Scala examples
Under each example root (java-udf/, scala-udf/, ...):
<br/>
```
mvn package
```

### Run them
Python UDF:
<br/>
```
spark2-submit --master local python-udf-example.py
```

Scala UDF:
<br/>
```
spark2-submit --class com.cloudera.fce.curtis.sparkudfexamples.scalaudf.ScalaUDFExample --master local target/scalaudf-0.0.1-jar-with-dependencies.jar
```

Java UDF:
<br/>
```
spark2-submit --class com.cloudera.fce.curtis.sparkudfexamples.javaudf.JavaUDFExample  --master local target/javaudf-0.0.1-jar-with-dependencies.jar
```

Scala UDAF:
<br/>
```
spark2-submit --class com.cloudera.fce.curtis.sparkudfexamples.scalaudaf.ScalaUDAFExample --master local target/scalaudaf-0.0.1-jar-with-dependencies.jar
```

Hive UDF:
<br/>
```
spark2-submit --jars target/hiveudf-0.0.1-jar-with-dependencies.jar  hive-udf-example.py
```

Scala UDAF From PySpark:
<br/>
```
spark2-submit --jars target/scalaudaffrompython-0.0.1.jar --driver-class-path target/scalaudaffrompython-0.0.1.jar scala-udaf-from-python.py
```
