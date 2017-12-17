from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Python UDF example").getOrCreate() 

df = spark.read.json("temperatures.json")
df.createOrReplaceTempView("citytemps")

# Register the UDF with our SparkSession 
spark.udf.register("CTOF", lambda degreesCelsius: ((degreesCelsius * 9.0 / 5.0) + 32.0))

spark.sql("SELECT city, CTOF(avgLow) AS avgLowF, CTOF(avgHigh) AS avgHighF FROM citytemps").show()
