# Databricks notebook source
# Student Data analysis

from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('MiniProject')
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/studentData.csv')
rdd.collect()

# COMMAND ----------

rdd1 = sc.textFile('/FileStore/tables/studentData.csv')
headers = rdd1.first()
rdd2 = rdd1.filter(lambda x: x != headers).map(lambda x: x.split(','))
rdd2.collect()

# COMMAND ----------

# Total Students

rdd2.count()

# COMMAND ----------

# Total marks by males and females

rdd3 = rdd2
rdd2.map(lambda x: (x[1], int(x[5]))).reduceByKey(lambda x,y: x+y).collect()

# COMMAND ----------

# Total passed and failed students

passed = rdd2.filter(lambda x: int(x[5]) > 50).count()
failed = rdd2.filter(lambda x: int(x[5]) <= 50).count()
print(f'passed: {passed}, failed: {failed}, total: {passed + failed}')

# COMMAND ----------

# total number enrolled per course

rdd2.map(lambda x: (x[3], 1)).collect()

# COMMAND ----------

ttl = rdd2.map(lambda x: (x[3], 1)).reduceByKey(lambda x,y: x + y).collect()
print(f'total number enrolled per course: {ttl}')

# COMMAND ----------

# total marks archieved per course

rdd2.map(lambda x: (x[3], int(x[5]))).collect()

# COMMAND ----------

ttl1 = rdd2.map(lambda x: (x[3], int(x[5]))).reduceByKey(lambda x,y: x + y).collect()
print(f'total marks archieved per course: {ttl1}')

# COMMAND ----------

# Average marks per course

avg = rdd2.map(lambda x: (x[3], (int(x[5]),1))).reduceByKey(lambda x,y : (x[0] + y[0], x[1] + y[1])) \
                                                .map(lambda x: (x[0], x[1][0] / x[1][1])).collect()
print(f'Average marks per course: {avg}')

# COMMAND ----------

# or with mapValues

avg = rdd2.map(lambda x: (x[3], (int(x[5]),1))).reduceByKey(lambda x,y : (x[0] + y[0], x[1] + y[1])) \
                                                .mapValues(lambda x: (x[0] / x[1])).collect()
print(f'Average marks per course: {avg}')

# COMMAND ----------

# Minimum and Maximum marks achieved per course

min_mark = rdd2.map(lambda x: (x[3], int(x[5]))).reduceByKey(lambda x,y: x if x > y else y).collect()
max_mark = rdd2.map(lambda x: (x[3], int(x[5]))).reduceByKey(lambda x,y: x if x < y else y).collect()

print(f'min_mark: {min_mark}, \n max_mark: {max_mark}')

# COMMAND ----------

# average age of male and female students

rdd2.map(lambda x: ( x[1], (int(x[0]), 1))).collect()

# COMMAND ----------

avg2 = rdd2.map(lambda x: ( x[1], (int(x[0]), 1))).reduceByKey(lambda x,y : (x[0] + y[0], x[1] + y[1]) ) \
                                            .mapValues(lambda x: x[0] / x[1]).collect()

print(f'average age of male and female students: {avg2}')
