# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("COLLABORATIVE FILTERING").getOrCreate()

# COMMAND ----------

movies_df = spark.read.csv("/FileStore/tables/movies.csv", header=True, inferSchema=True)
ratings_df = spark.read.csv("/FileStore/tables/ratings.csv", header=True, inferSchema=True)

movies_df.show(5)
ratings_df.show(5)

# COMMAND ----------

display(movies_df)

# COMMAND ----------

display(ratings_df)

# COMMAND ----------

df = ratings_df.join(movies_df, "movieId", "left")

# COMMAND ----------

# Train-test-data

(train_df, test_df) = df.randomSplit([0.8, 0.2])

# COMMAND ----------

train_df.show(5)

# COMMAND ----------

test_df.show(5)

# COMMAND ----------

# ALS Model

from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

als = ALS(userCol = "userId", itemCol = "movieId", ratingCol = "rating", nonnegative=True, implicitPrefs=False, coldStartStrategy="drop")

# COMMAND ----------

# Hyperparameter Tuning and Cross-validation

param_grid = ParamGridBuilder() \
              .addGrid(als.rank, [10, 50, 100, 150]) \
              .addGrid(als.regParam, [.01, .05, .1, .15]) \
              .build()

# COMMAND ----------

evaluator = RegressionEvaluator(
            metricName="rmse",
            labelCol="rating",
            predictionCol="prediction") 

# COMMAND ----------

cv = CrossValidator(
      estimator=als,
      estimatorParamMaps=param_grid,
      evaluator=evaluator,
      numFolds=5)

# COMMAND ----------

model = cv.fit(train_df)
best_model = model.bestModel
testPredictions = best_model.transform(test)
RMSE = evaluator(testPredictions)

print(RMSE)

# COMMAND ----------

# Recomendations

recommend_df = best_model.recommendForAllUsers(5)
display(recommend_df)

# COMMAND ----------

df2 = recommend_df.withColumn("movie_rating", explode('recommendations'))
df2.select("userId", col("movieid_rating"), col("movieid_rating.rating"))
