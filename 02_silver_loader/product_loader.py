# Databricks notebook source
# Imports
from datetime import datetime
from pyspark.sql.functions import current_timestamp, lit

# Print start message with actual timestamp
print(f"Product Silver Load Process started at: {datetime.now()}")

# Read data from the Bronze table
bronze_products_df = spark.read.table("oms_analytics.bronze.product")

# Transformations for Silver
products_silver_df = bronze_products_df \
    .drop("bronze_load_ts") \
    .withColumn("silver_load_ts", current_timestamp()) \
    .withColumn("process_id", lit("de_nb_102"))

# Write transformed data to the Delta table in oms_analytics.02_silver
products_silver_df.write.format("delta").mode("overwrite").saveAsTable("oms_analytics.silver.product")

# Print end message with actual timestamp
print(f"Product Silver Load Process completed at: {datetime.now()}")

