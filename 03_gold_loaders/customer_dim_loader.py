# Databricks notebook source
from datetime import datetime
from pyspark.sql.functions import current_timestamp, lit

# Print start message with actual timestamp
print(f"Customer Gold Load Process started at: {datetime.now()}")

# Read data from the Silver table
customer_silver_df = spark.read.table("oms_analytics.silver.customers")

# Transformations for Gold
customer_gold_df = customer_silver_df \
    .withColumnRenamed("customer_name", "name") \
    .withColumnRenamed("customer_email", "email") \
    .withColumnRenamed("customer_mobile", "mobile") \
    .withColumnRenamed("silver_load_ts", "gold_load_ts") \
    .withColumn("process_id", lit("de_nb_102")) \
    .withColumn("gold_load_ts", current_timestamp())  # Set current timestamp for the gold_load_ts

# Select only the required columns for the Gold table
columns_to_select = [
    "customer_id",
    "name",
    "email",
    "mobile",
    "process_id",
    "gold_load_ts"
]

# Final DataFrame selection
customer_gold_df = customer_gold_df.select(*columns_to_select)

# Write transformed data to the Delta table in oms_analytics.gold
customer_gold_df.write.format("delta").mode("overwrite").saveAsTable("oms_analytics.gold.customer_dim")

# Print end message with actual timestamp
print(f"Customer Gold Load Process completed at: {datetime.now()}")

