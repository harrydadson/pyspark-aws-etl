# Databricks notebook source
from pyspark.sql import SparkSession

from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import IntegerType, DoubleType 
spark = SparkSession.builder.appName('Spark Dataframe').getOrCreate()

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/studentData.csv', header=True, inferSchema=True)
df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
  StructField("age", IntegerType(), True),
  StructField("gender", StringType(), True),
  StructField("name", StringType(), True),
  StructField("course", StringType(), True),
  StructField("roll", StringType(), True),
  StructField("marks", IntegerType(), True),
  StructField("email", StringType(), True)
])

# COMMAND ----------

df1 = spark.read.csv('/FileStore/tables/studentData.csv', header=True, schema=schema ,inferSchema=True)
df1.show()
df1.printSchema()

# COMMAND ----------

# Create dataframe from rdd

from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Read File")
sc = SparkContext.getOrCreate(conf=conf)

rdd = sc.textFile('/FileStore/tables/studentData.csv')
headers = rdd.first()
rdd = rdd.filter(lambda x: x != headers).map(lambda x: x.split(',')) \
                                        .map(lambda x: [int(x[0]), x[1], x[2], x[3], x[4], int(x[5]), x[6]]).collect()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
  StructField("age", IntegerType(), True),
  StructField("gender", StringType(), True),
  StructField("name", StringType(), True),
  StructField("course", StringType(), True),
  StructField("roll", StringType(), True),
  StructField("marks", IntegerType(), True),
  StructField("email", StringType(), True)
])

# COMMAND ----------

# cols = headers.split(',')
# dfRdd = rdd.toDF(cols)
dfRdd = spark.createDataFrame(rdd, schema=schema)
dfRdd.show()

# COMMAND ----------

# Select Cols

df = spark.read.csv('/FileStore/tables/studentData.csv', header=True, inferSchema=True)
df.show()

# COMMAND ----------

df.select('gender','name').show(5)

# COMMAND ----------

df.select(df.columns[:3]).show(5)

# COMMAND ----------

# withColumn

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

df = df.withColumn('roll', col("roll").cast("String"))
df.printSchema()

# COMMAND ----------

df.withColumn('marks', col('marks') + 10).show()

# COMMAND ----------

df.withColumn('aggregated marks', col('marks') - 10).show()

# COMMAND ----------

df.withColumn('Country', lit('USA')).show()

# COMMAND ----------

# withColumnRenamed() and alias

df.withColumnRenamed('gender', 'sex').show(5)

# COMMAND ----------

df.select(col('name').alias('full name')).show(5)

# COMMAND ----------

# filter() and where()

df.filter(col('course') == 'DB').show(5)

# COMMAND ----------

df.filter( (col('course') == 'DB') & (col('marks') > 50) ).show(5) # | -> or

# COMMAND ----------

courses = ['DB', 'Cloud', 'OOP', 'DSA']
df.filter(col('course').isin(courses)).show()

# COMMAND ----------

df.filter(col('course').startswith('D')).show() # endswith(), contains(), like('%..%')

# COMMAND ----------

# total marks

df.withColumn('total_marks', lit(120))

# COMMAND ----------

# average marks

df.withColumn('average', (col('marks') / col('total_marks')) * 100)

# COMMAND ----------

# filter out students who have more than 80% marks in OOP course

df.filter((col('course') == 'OOP') & (col('average') > 80))

# COMMAND ----------

# filter out students who have more than 60% marks in cloud course

df.filter((col('course') == 'Cloud') & (col('average') > 60))

# COMMAND ----------

# count(), distinct(), dropDuplicates()

df = spark.read.csv('/FileStore/tables/studentData.csv', header=True, inferSchema=True)
df.show(5)

# COMMAND ----------

# Count

df.filter(col('course') == 'DB').count()

# COMMAND ----------

# distinct()

df.select('gender','age').distinct().show()

# COMMAND ----------

df.dropDuplicates(['gender','age']).show()

# COMMAND ----------

df.select('age', 'gender', 'course').distinct().show()

# COMMAND ----------

# sort/orderBy

df.sort('marks', 'age').show(5)

# COMMAND ----------

df.orderBy(col('marks').asc(),col('age').desc()).show(5)

# COMMAND ----------

office_df = spark.read.csv('/FileStore/tables/officeData.csv', header=True, inferSchema=True)
office_df.show(5)

# COMMAND ----------

# bonus of emp ascending

office_df.orderBy(col('bonus').asc()).show(5)

# COMMAND ----------

# sort in age and salary in desc and asc respectively

office_df.orderBy(col('age').desc(), col('salary').asc()).show(5)

# COMMAND ----------

# groupby() - has to be done with an aggregations

df.groupby('gender').sum('marks').show()

# COMMAND ----------

df.groupby('gender').count().show() 
df.groupby('gender').max('marks').show() 
df.groupby('gender').min('marks').show() 
df.groupby('gender').avg('marks').show()
df.groupby('gender').mean('marks').show() 

# COMMAND ----------

# groupby() - multiple cols aggregation

df.groupBy('course', 'gender').count().show()

# COMMAND ----------

from pyspark.sql.functions import sum, avg, max, min, mean, count

df.groupBy('course','gender').agg(count('*').alias('total_enrollments'), sum('marks'), min('marks'), max('marks'), avg('marks')).show()

# COMMAND ----------

# groupBy() - filter()

df.filter(col('gender') == 'Male').groupBy('gender').agg(count('*')).show()

# COMMAND ----------

df.filter(col('gender') == 'Male').groupBy('course', 'gender').agg(count('*').alias('total_enrollments')) \
                                                              .filter(col('total_enrollments') > 85).show()

# COMMAND ----------

# total # of students enrolled in each course

df.groupBy('course').count().show()

# COMMAND ----------

# total # of male and female students in each course

df.groupBy('gender','course').count().show()

# COMMAND ----------

df.groupBy('gender','course').agg(sum('marks')).show()

# COMMAND ----------

df.groupBy('age','course').agg(min('marks'), max('marks'), avg('marks')).show()

# COMMAND ----------

# Word Count

wrd_df = spark.read.csv('/FileStore/tables/wordData.txt', header=False, inferSchema=True)
wrd_df.show(5)

# COMMAND ----------

# calculate and show count of each word present in file

wrd_df.groupBy('_c0').count().show()

# COMMAND ----------

# UDF - user-defined func
def get_ttl_salary(salary, bonus):
  return salary + bonus

ttlSalary = udf(lambda x,y: get_ttl_salary(x,y), IntegerType())

office_df.withColumn('total_salary', ttlSalary(col('salary'), col('bonus'))).show()

# COMMAND ----------

# if employee from NY, increment 10% of salary plus 5% bonus
# if employee from CA, increment 12% of salary plus 3% bonus

def get_incr(state, salary, bonus):
  if state == 'NY':
    sum = salary * 0.10
    sum += bonus * 0.05
  elif state == 'CA':
    sum = salary * 0.12
    sum += bonus * 0.03
  return sum

incrUDF = udf(lambda x,y,z: get_incr(x,y,z), DoubleType())

office_df.withColumn("increment", incrUDF(col('state'), col('salary'), col('bonus'))).show()

# COMMAND ----------

# cache and persist

# cache() picks up from last ran transformation

# COMMAND ----------

# SparkSQL

df.createOrReplaceTempView('Student')

spark.sql("select * from Student").show()

# COMMAND ----------

spark.sql(
"SELECT course, gender, sum(marks) \
FROM Student \
GROUP BY course, gender"
).show()

# COMMAND ----------

# WriteDF

df.write.csv('/FileStore/tables/studentData/output', header=True)

# COMMAND ----------

#Project

# Total # of employees

proj_df =  spark.read.csv('/FileStore/tables/officeDataProject.csv', header=True, inferSchema=True)
proj_df.count()

# COMMAND ----------

# total # of departments

proj_df.select('department').dropDuplicates(['department']).count()

# COMMAND ----------

# names of dept

proj_df.select('department').dropDuplicates(['department']).show()

# COMMAND ----------

# total #s of employees in each department

proj_df.groupBy('department').count().show()

# COMMAND ----------

# total # of employes in each state

proj_df.groupBy('state').count().show()

# COMMAND ----------

# total # of employees in each state in each department

proj_df.groupBy('state','department').count().show()

# COMMAND ----------

# min, max salary

proj_df.groupBy("department").agg(min("salary").alias("min"), max("salary").alias("max")).orderBy(col("max").asc(), col("min").asc()).show()

# COMMAND ----------

# names of emp in NY under finance with bonuses > avg bonuses of emps in NY

avgBonus = proj_df.filter(col("state") == "NY").groupBy(col("state")).agg(avg("bonus").alias("avg_bonus")).select("avg_bonus").collect()[0]['avg_bonus']
proj_df.filter((col("state") == "NY") & (col("department") == "Finance") & (col("bonus") > avgBonus)).show()

# COMMAND ----------

# raise salaries $500 for emps age > 45

def incr_salary(age, currentSalary):
  if age > 45:
    return currentSalary + 500
  return currentSalary

incrSalaryUDF = udf(lambda x,y : incr_salary(x,y), IntegerType())


proj_df.withColumn("salary", incrSalaryUDF(col("age"), col("salary"))).show()

# COMMAND ----------

proj_df.filter(proj_df.age > 45).write.csv("/FileStore/tables/output_45")

# COMMAND ----------


