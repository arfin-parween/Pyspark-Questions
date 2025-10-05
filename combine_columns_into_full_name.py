# Databricks notebook source
from pyspark.sql import functions as F
data = [("Arfin", "Parween"), ("Gopal", "Krishna"), ("Puja", "Sharma")]
columns = ["first_name", "last_name"]
df = spark.createDataFrame(data, columns)
display(df)

# COMMAND ----------

def add_full_name(df, first="first_name", last="last_name"):
    return df.withColumn("full_name", F.concat_ws(" ", F.col(first), F.col(last)))
result_df = add_full_name(df)
display(result_df)