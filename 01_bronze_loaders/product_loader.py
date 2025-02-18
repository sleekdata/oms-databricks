# Databricks notebook source
# Imports
from datetime import datetime
from pyspark.sql import functions as F

# Print start message with actual timestamp
print(f"Product Silver Load Process started at: {datetime.now()}")

# Read data from the federated PostgreSQL table
product_df = spark.read.table("ext_postgres_db.oms_schema.product")

# Transforms
product_transformed_df = product_df \
    .drop("load_ts") \
    .withColumn("bronze_load_ts", F.current_timestamp()) \
    .withColumn("process_id", F.lit("de_nb_102"))

# Write transformed data to the Delta table in oms_analytics_01_bronze
product_transformed_df.write.format("delta").mode("overwrite").saveAsTable("oms_analytics.bronze.product")

# Print end message with actual timestamp
print(f"Product Silver Load Process completed at: {datetime.now()}")

