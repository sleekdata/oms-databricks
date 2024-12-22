# Databricks notebook source
from datetime import datetime
from pyspark.sql.functions import current_timestamp, lit

# Print start message with actual timestamp
print(f"Date Gold Load Process started at: {datetime.now()}")

# Read data from the Silver table (oms_analytics.02_silver.dates)
silver_dates_df = spark.read.table("oms_analytics.02_silver.dates")

# Transformations for Gold
dates_gold_df = silver_dates_df \
    .withColumnRenamed("silver_load_ts", "gold_load_ts") \
    .withColumn("process_id", lit("de_nb_102")) \
    .withColumn("gold_load_ts", current_timestamp())  # Set current timestamp for the gold_load_ts

# Write transformed data to the Delta table in oms_analytics.03_gold
dates_gold_df.write.format("delta").mode("overwrite").saveAsTable("oms_analytics.03_gold.date_dim")

# Print end message with actual timestamp
print(f"Date Gold Load Process completed at: {datetime.now()}")

