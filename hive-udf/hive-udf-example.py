from pyspark     import SparkConf, SparkContext
from pyspark.sql import HiveContext

conf       = SparkConf().setAppName("Hive UDF example")
sc         = SparkContext(conf=conf)
sqlContext = HiveContext(sc)

df         = sqlContext.read.json("temperatures.json")
df.registerTempTable("citytemps")

# Register our Hive UDF
sqlContext.sql("CREATE TEMPORARY FUNCTION CtoF AS 'com.cloudera.fce.curtis.sparkudfexamples.hiveudf.CtoF'")

sqlContext.sql("SELECT city, CTOF(avgLow) AS avgLow, CTOF(avgHigh) AS avgHigh FROM citytemps").show()
