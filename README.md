# sparkudfexamples
Simple examples of Spark SQL user-defined functions

<code>
spark-submit --class com.cloudera.fce.curtis.sparkudfexamples.javaudf.JavaUDFExample  --master local target/javaudf-0.0.1-jar-with-dependencies.jar
</code>
<br/>
<code>
spark-submit --class com.cloudera.fce.curtis.sparkudfexamples.scalaudf.ScalaUDFExample --master local target/scalaudf-0.0.1-jar-with-dependencies.jar
</code>
<br/>
<code>
spark-submit --master local python-udf-example.py
</code>
<br/>
<code>
spark-submit --class com.cloudera.fce.curtis.sparkudfexamples.scalaudaf.ScalaUDAFExample --master local target/scalaudaf-0.0.1-jar-with-dependencies.jar
</code>

