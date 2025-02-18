# Databricks notebook source
# Imports
from datetime import datetime
from pyspark.sql import functions as F

# Print start message with actual timestamp
print(f"Customer Silver Load Process started at: {datetime.now()}")

# Read data from the federated PostgreSQL table
customers_df = spark.read.table("ext_postgres_db.oms_schema.customers")

# Transforms
customers_transformed_df = customers_df \
    .drop("load_ts") \
    .withColumn("process_id", F.lit("de_nb_102")) \
    .withColumn("bronze_load_ts", F.current_timestamp())


# Write transformed data to the Delta table in oms_analytics_01_bronze
customers_transformed_df.write.format("delta").mode("overwrite").saveAsTable("oms_analytics.bronze.customers")

# Print end message with actual timestamp
print(f"Customer Silver Load Process completed at: {datetime.now()}")
