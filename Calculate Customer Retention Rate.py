# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

data = [
    ("c1", "2025-01-05"),
    ("c2", "2025-01-10"),
    ("c1", "2025-02-15"),
    ("c3", "2025-02-20"),
    ("c2", "2025-03-05")
]
columns = ["customer_id", "purchase_date"]

df = spark.createDataFrame(data, columns)

df = df.withColumn("purchase_date", F.to_date("purchase_date"))

display(df)

df = df.withColumn("purchase_month", F.date_format("purchase_date", "yyyy-MM"))

first_purchase_df = df.groupBy("customer_id").agg(
    F.min("purchase_month").alias("first_month")
)

df = df.join(first_purchase_df, on="customer_id", how="inner")
df = df.withColumn(
    "next_month",
    F.date_format(F.add_months(F.to_date(F.concat(F.col("first_month"), F.lit("-01"))), 1), "yyyy-MM")
)

retained_df = df.filter(F.col("purchase_month") == F.col("next_month")).select("customer_id").distinct()

total_customers = first_purchase_df.count()
retained_customers = retained_df.count()
retention_rate = (retained_customers / total_customers) * 100

print(f"Retention Rate: {retention_rate:.2f}%")

final_df = spark.createDataFrame(
    [(round(retention_rate, 2),)],
    ["retention_rate (%)"]
)
display(final_df)
