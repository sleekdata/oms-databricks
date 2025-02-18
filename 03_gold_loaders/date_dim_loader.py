# Databricks notebook source
from datetime import datetime
from pyspark.sql.functions import current_timestamp, lit

# Print start message with actual timestamp
print(f"Date Gold Load Process started at: {datetime.now()}")

# Read data from the Silver table (oms_analytics.silver.dates)
silver_dates_df = spark.read.table("oms_analytics.silver.dates")

# Transformations for Gold
dates_gold_df = silver_dates_df \
    .withColumn("process_id", lit("de_nb_102")) \
    .withColumn("gold_load_ts", current_timestamp())

# Write transformed data to the Delta table in oms_analytics.gold
dates_gold_df.write.format("delta").mode("overwrite").saveAsTable("oms_analytics.gold.date_dim")

# Print end message with actual timestamp
print(f"Date Gold Load Process completed at: {datetime.now()}")

