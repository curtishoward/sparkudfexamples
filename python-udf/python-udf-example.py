from pyspark     import SparkConf, SparkContext
from pyspark.sql import SQLContext

conf       = SparkConf().setAppName("Python UDF example")
sc         = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

df         = sqlContext.read.json("temperatures.json")
df.registerTempTable("citytemps")

# Register the UDF with our SQLContext
sqlContext.registerFunction("CTOF", lambda degreesCelsius: ((degreesCelsius * 9.0 / 5.0) + 32.0))

sqlContext.sql("SELECT city, CTOF(avgLow) AS avgLowF, CTOF(avgHigh) AS avgHighF FROM citytemps").show()
