from pyspark     import SparkConf, SparkContext
from pyspark.sql import SQLContext

conf       = SparkConf().setAppName("Python UDF example")
sc         = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

df         = sqlContext.read.json("temperatures.json")
df.registerTempTable("citytemps")

sqlContext.registerFunction("CTOF", lambda celcius: ((celcius * 9.0 / 5.0) + 32.0))

sqlContext.sql("SELECT city, CTOF(avgLow) AS avgLow, CTOF(avgHigh) AS avgHigh FROM citytemps").show()
