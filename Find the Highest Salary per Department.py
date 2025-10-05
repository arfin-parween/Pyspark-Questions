# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
data = [
    ("Arfin", "IT", 90000),
    ("Gopal", "HR", 60000),
    ("Puja", "IT", 85000),
    ("Sneha", "HR", 95000),
    ("Raj", "Finance", 88000)
]
columns = ["name", "department", "salary"]
df = spark.createDataFrame(data, columns)

# COMMAND ----------

window_spec = Window.partitionBy("department").orderBy(F.desc("salary"))
ranked_df = df.withColumn("rank", F.row_number().over(window_spec))
top_salary_df = ranked_df.filter(F.col("rank") == 1).drop("rank")
display(top_salary_df)
