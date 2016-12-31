# sparkudfexamples
Simple examples of Spark SQL user-defined functions

<code>
spark-submit --class com.cloudera.fce.curtis.sparkudfexamples.javaudfexample.JavaUDFExample  --master local target/javaudfexample-0.0.1-jar-with-dependencies.jar
</code>
<br/>
<code>
spark-submit --class com.cloudera.scalaudfexample.ScalaUDFSample --master local target/scalaudfexample-0.0.1-SNAPSHOT.jar
</code>
<br/>
<code>
spark-submit                                                     --master local python-udf.py
</code>
