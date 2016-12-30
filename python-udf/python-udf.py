from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
APP_NAME = "Python UDF example"

#def CtoF(celcius):
#  ((x*9.0/5.0)+32.0)

conf = SparkConf().setAppName(APP_NAME)
conf = conf.setMaster("local[*]")
sc   = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
trd    = sc.parallelize([(1,'a'),(2,'b'),(3,'c'),(4,'d')])
testDF = trd.toDF(['numVal','strVal'])
testDF.registerTempTable("testDF")
sqlContext.registerFunction("CURTIS", lambda x: ((x*9.0/5.0)+32.0))
sqlContext.sql("SELECT CURTIS(numVal) AS Fahrenheit FROM testDF").show()
