# Databricks notebook source
print("Hello")

# COMMAND ----------

from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Read File")
sc = SparkContext.getOrCreate(conf=conf)
text = sc.textFile('/FileStore/tables/sample.txt')

# COMMAND ----------

text.collect()

# COMMAND ----------

## Map

text.map(lambda x: x.split(" ")).collect()

# COMMAND ----------

def txtFunc(x):
  
  l2 = x.split()
  lst = []
  for s in l2:
    lst.append(int(s) + 2)
  return lst

text.map(txtFunc).collect()
  

# COMMAND ----------

text1 = sc.textFile('/FileStore/tables/sample1.txt')
text1.collect()

# COMMAND ----------

def countTxt(x):
  lst1 = []
  l = x.split(" ")
  for s in l:
    lst1.append(len(s))

  return lst1


text1.map(countTxt).collect()
  
  
  

# COMMAND ----------

text1.map(lambda x: [len(s) for s in x.split(' ')]).collect()

# COMMAND ----------

## FlatMap

text = sc.textFile('/FileStore/tables/sample.txt')

text.flatMap(lambda x: x.split(" ")).collect()

# COMMAND ----------

# Filter()

text.collect()

# COMMAND ----------

text.filter(lambda x: x != '12 12 33').collect()

# COMMAND ----------

def filterFunc(x):
  if x == '12 12 33':
    return False
  else:
    return True
  
text.filter(filterFunc).collect()

# COMMAND ----------

text2 = sc.textFile('/FileStore/tables/sample2.txt')
text2.collect()

# COMMAND ----------

text2.flatMap(lambda x: x.split(' ')).filter(lambda x: not (x.startswith('a') or x.startswith('c'))).collect()

# COMMAND ----------

# Distinct()

text.collect()

# COMMAND ----------

text.flatMap(lambda x: x.split(' ')).distinct().collect()

# COMMAND ----------

# groupByKey()

text2 = sc.textFile('/FileStore/tables/sample2.txt')
text2.collect()

# COMMAND ----------

text2.flatMap(lambda x: x.split(' ')).map(lambda x: (x, len(x))).groupByKey().mapValues(list).collect()

# COMMAND ----------

# reduceByKey()

text2 = sc.textFile('/FileStore/tables/sample2.txt')
text2.collect()

# COMMAND ----------

text2.flatMap(lambda x: x.split(' ')).map(lambda x: (x, len(x))).groupByKey().mapValues(list).collect() # (ant, [3,3]) -> (ant, 6)

# COMMAND ----------

text3 = sc.textFile('/FileStore/tables/sample3.txt')
text3.collect()

# COMMAND ----------

text3.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).groupByKey().mapValues(list).collect()

# COMMAND ----------

text3.flatMap(lambda x: x.split(' ')).map(lambda x: (x,1)).reduceByKey(lambda x,y : x + y).collect()

# COMMAND ----------

# CountbyValue

text4 = sc.textFile('/FileStore/tables/sample3.txt')
text4.collect()

# COMMAND ----------

text4.flatMap(lambda x: x.split(' ')).countByValue()

# COMMAND ----------

# saveAsTextFile

text4.saveAsTextFile('/FileStore/tables/output/sample_out.txt')

# COMMAND ----------

# repartition() - to change # of partitions in RDD (rdd.repartition(#ofpartitions))

text4 = sc.textFile('/FileStore/tables/sample3.txt')
#text4.getNumPartitions() -> 2
rdd = text4.repartition(5).flatMap(lambda x: x.split(' ')).map(lambda x: (x,1))
# rdd.saveAsTextFile('/FileStore/tables/output/sample_out_1.txt')

# COMMAND ----------

# coalesce() - to decrese # of partitions in RDD (rdd.coalesce())

rdd.getNumPartitions()

# COMMAND ----------

rdd1 = rdd.coalesce(3)
rdd1.saveAsTextFile('/FileStore/tables/output/sample_out_2.txt')

# COMMAND ----------

# finding average

text5 = sc.textFile('/FileStore/tables/movie_ratings.csv')
text5.collect()

# COMMAND ----------

text5.map(lambda x: (x.split(',')[0], int(x.split(',')[1]))).collect()

# COMMAND ----------

avrg = text5.map(lambda x: (x.split(',')[0], (int(x.split(',')[1]),1) ))
avrg.collect()

# COMMAND ----------

avrg1 = avrg.reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
avrg1.collect()

# COMMAND ----------

avrg1.map(lambda x: (x[0], x[1][0]/x[1][1])).collect()

# COMMAND ----------

text6 = sc.textFile('/FileStore/tables/avg_quiz_sample.csv')
text6.collect()

# COMMAND ----------

text6.map(lambda x: (x.split(',')[0], (float(x.split(',')[2]),1))).collect()

# COMMAND ----------

text6.map(lambda x: (x.split(',')[0], (float(x.split(',')[2]),1))).reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1])) \
                                                                  .map(lambda x: (x[0],x[1][0] / x[1][1])).collect()

# COMMAND ----------

# Min and Max

text7 = sc.textFile('/FileStore/tables/movie_ratings.csv')
text7.collect()

# COMMAND ----------

text7.map(lambda x: (x.split(',')[0], int(x.split(',')[1]))).collect()

# COMMAND ----------

text7.map(lambda x: (x.split(',')[0], int(x.split(',')[1]))).reduceByKey(lambda x,y: x if x < y else y).collect()

# COMMAND ----------

from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Read File")
sc = SparkContext.getOrCreate(conf=conf)
text = sc.textFile('/FileStore/tables/sample.txt')




