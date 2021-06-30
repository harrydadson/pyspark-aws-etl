# Databricks notebook source
# Streaming for RDD

from pyspark.streaming import StreamingContext
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('streaming')
sc = SparkContext.getOrCreate(conf=conf)

ssc = StreamingContext(sc, 1)

# COMMAND ----------

rdd = ssc.textFileStream("/FileStore/tables/")

# COMMAND ----------

rdd = rdd.map(lambda x: (x, 1))
rdd.reduceByKey(lambda x,y: x + y)

rdd.pprint()

ssc.start()
ssc.awaitTerminationOrTimeout(100)

# COMMAND ----------

# Spark-Streaming - DataFrames
# dbutils.fs.rm("/FileStore/tables", True)

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark Streaming DF").getOrCreate()

words = spark.readStream.text("/FileStore/tables")

# COMMAND ----------

# words.writeStream.format("console").outputMode("complete").start()

words.groupBy("value").count()
display(words)

# COMMAND ----------


