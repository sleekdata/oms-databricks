# Databricks notebook source
from datetime import datetime
from pyspark.sql.functions import current_timestamp, lit

# Print start message with actual timestamp
print(f"Product Category Silver Load Process started at: {datetime.now()}")

# Read data from the Bronze table
bronze_category_df = spark.read.table("oms_analytics.01_bronze.product_category")

# Transformations for Silver
category_silver_df = bronze_category_df \
    .drop("bronze_load_ts") \
    .withColumn("silver_load_ts", current_timestamp()) \
    .withColumn("process_id", lit("de_nb_102"))

# Write transformed data to the Delta table in oms_analytics.02_silver
category_silver_df.write.format("delta").mode("overwrite").saveAsTable("oms_analytics.02_silver.product_category")

# Print end message with actual timestamp
print(f"Product Category Silver Load Process completed at: {datetime.now()}")

