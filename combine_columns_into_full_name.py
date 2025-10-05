# Databricks notebook source
from pyspark.sql import functions as F
data = [("Arfin", "Parween"), ("Gopal", "Krishna"), ("Puja", "Sharma")]
columns = ["first_name", "last_name"]
df = spark.createDataFrame(data, columns)
display(df)

# COMMAND ----------

result_df = df.withColumn(
    "full_name",
    F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
)
display(result_df)
