# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, to_date, date_format, dayofweek, month, year, quarter, when, current_timestamp
)
from pyspark.sql.types import IntegerType
from datetime import datetime, timedelta

# Print start message with actual timestamp
print(f"Date Dimension Load Process started at: {datetime.now()}")

# Create a range of dates for 2024 and 2025
start_date = datetime(2024, 1, 1)
end_date = datetime(2025, 12, 31)
date_range = [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') 
              for i in range((end_date - start_date).days + 1)]

# Create a Spark DataFrame from the date range
spark = SparkSession.builder.getOrCreate()
dates_df = spark.createDataFrame(date_range, "string").toDF("date")

# Transform the DataFrame to include required columns
dates_transformed_df = dates_df \
    .withColumn("date_id", date_format(col("date"), "yyyyMMdd").cast(IntegerType())) \
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
    .withColumn("day_of_week", dayofweek(col("date"))) \
    .withColumn("month", month(col("date"))) \
    .withColumn("year", year(col("date"))) \
    .withColumn("quarter", quarter(col("date"))) \
    .withColumn("is_weekend", when(col("day_of_week").isin(1, 7), lit(1)).otherwise(lit(0))) \
    .withColumn("process_id", lit("de_nb_102")) \
    .withColumn("silver_load_ts", current_timestamp())

# Write the transformed data to the Delta table in oms_analytics.02_silver
dates_transformed_df.write.format("delta").mode("overwrite").saveAsTable("oms_analytics.02_silver.dates")

# Print end message with actual timestamp
print(f"Date Dimension Load Process completed at: {datetime.now()}")

