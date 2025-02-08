# Databricks notebook source
from datetime import datetime
from pyspark.sql.functions import current_timestamp

# Print start message with actual timestamp
print(f"Product Category Silver Load Process started at: {datetime.now()}")

# Read data from the federated PostgreSQL table
product_category_df = spark.read.table("ext_postgres_db.oms_schema.product_category")

# Transforms
product_category_transformed_df = product_category_df \
    .drop("load_ts") \
    .withColumn("bronze_load_ts", current_timestamp()) \
    .withColumn("process_id", lit("de_nb_102"))

# Write transformed data to the Delta table in oms_analytics_01_bronze
product_category_transformed_df.write.format("delta").mode("overwrite").saveAsTable("oms_analytics.bronze.product_category")

# Print end message with actual timestamp
print(f"Product Category Silver Load Process completed at: {datetime.now()}")

