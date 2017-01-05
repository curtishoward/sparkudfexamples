from pyspark     import SparkConf, SparkContext
from pyspark.sql import HiveContext

conf       = SparkConf().setAppName("Hive UDF example")
sc         = SparkContext(conf=conf)
sqlContext = HiveContext(sc)

df         = sqlContext.read.json("temperatures.json")
df.registerTempTable("citytemps")

# Register our Hive UDF
sqlContext.sql("CREATE TEMPORARY FUNCTION CTOF AS 'com.cloudera.fce.curtis.sparkudfexamples.hiveudf.CTOF'")

sqlContext.sql("SELECT city, CTOF(avgLow) AS avgLowF, CTOF(avgHigh) AS avgHighF FROM citytemps").show()
