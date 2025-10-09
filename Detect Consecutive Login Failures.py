# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType
data = [
    ("u1", "2025-01-01 10:00:00", "fail"),
    ("u1", "2025-01-01 10:05:00", "fail"),
    ("u1", "2025-01-01 10:20:00", "fail"),
    ("u1", "2025-01-01 11:00:00", "success"),
    ("u2", "2025-01-01 09:00:00", "fail"),
    ("u2", "2025-01-01 09:45:00", "fail"),
    ("u2", "2025-01-01 09:50:00", "fail"),
    ("u3", "2025-01-01 08:00:00", "success")
]
schema = StructType([
    StructField("user_id", StringType()),
    StructField("event_time", StringType()),  
    StructField("status", StringType())
])
df = spark.createDataFrame(data, schema)


# COMMAND ----------

df = df.withColumn("event_time", F.to_timestamp("event_time"))
display(df.orderBy("user_id", "event_time"))
fail_df = df.filter(F.col("status") == "fail")
w = Window.partitionBy("user_id").orderBy("event_time")
fail_df = fail_df.withColumn("prev1", F.lag("event_time", 1).over(w)) \
                 .withColumn("prev2", F.lag("event_time", 2).over(w))
fail_df = fail_df.withColumn(
    "within_30min",
    F.when(
        (F.col("prev2").isNotNull()) &
        ((F.unix_timestamp("event_time") - F.unix_timestamp("prev2")) <= 1800),
        True
    ).otherwise(False)
)
locked_users = fail_df.filter("within_30min").select("user_id").distinct()
display(fail_df)
display(locked_users)