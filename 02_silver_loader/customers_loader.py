# Databricks notebook source
from datetime import datetime
from pyspark.sql.functions import current_timestamp, lit

# Print start message with actual timestamp
print(f"Customer Silver Load Process started at: {datetime.now()}")

# Read data from the Bronze table
bronze_customers_df = spark.read.table("oms_analytics.01_bronze.customers")

# Transformations for Silver
customers_silver_df = bronze_customers_df \
    .withColumnRenamed("bronze_load_ts", "silver_load_ts") \
    .withColumn("process_id", lit("de_nb_102")) \
    .withColumn("silver_load_ts", current_timestamp())

# Write transformed data to the Delta table in oms_analytics.02_silver
customers_silver_df.write.format("delta").mode("overwrite").saveAsTable("oms_analytics.02_silver.customers")

# Print end message with actual timestamp
print(f"Customer Silver Load Process completed at: {datetime.now()}")

