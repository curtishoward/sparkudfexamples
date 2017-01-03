from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("Scala UDAF from Python example")
sc   = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

df = sqlContext.read.json("inventory.json")
df.registerTempTable("inventory")

scala_sql_context  =  sqlContext._ssql_ctx
scala_spark_context = sqlContext._sc
scala_spark_context._jvm.com.cloudera.fce.curtis.sparkudfexamples.scalaudaffrompython.ScalaUDAFFromPythonExample.registerUdf(scala_sql_context)

sqlContext.sql("SELECT Make, SUMPRODUCT(RetailValue,Stock) as InventoryValuePerMake FROM inventory GROUP BY Make").show()
