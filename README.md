# sparkudfexamples
Simple examples of Spark SQL user-defined functions

<code>
spark-submit --class com.cloudera.javaudfexample.JavaUDFExample --master local target/javaudfexample-0.0.1-SNAPSHOT.jar inputfile.txt 2
spark-submit --class com.cloudera.udfexample.UDFExample         --master local target/udfexample-0.0.1-SNAPSHOT.jar     inputfile.txt 2
spark-submit                                                    --master local python-udf.py
</code>
