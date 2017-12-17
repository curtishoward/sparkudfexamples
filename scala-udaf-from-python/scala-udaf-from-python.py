from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Scala UDAF from Python example").getOrCreate()

df = spark.read.json("inventory.json")
df.createOrReplaceTempView("inventory")

spark.sparkContext._jvm.com.cloudera.fce.curtis.sparkudfexamples.scalaudaffrompython.ScalaUDAFFromPythonExample.registerUdf()

spark.sql("SELECT Make, SUMPRODUCT(RetailValue,Stock) as InventoryValuePerMake FROM inventory GROUP BY Make").show()
