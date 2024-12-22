# Databricks notebook source
from datetime import datetime
from pyspark.sql.functions import current_timestamp, lit

# Print start message with actual timestamp
print(f"Customer Silver Load Process started at: {datetime.now()}")

# Read data from the federated PostgreSQL table
customers_df = spark.read.table("postgres_federated_db.oms_schema.customers")

# Transforms
customers_transformed_df = customers_df \
    .drop("load_ts") \
    .withColumn("process_id", lit("de_nb_102")) \
    .withColumn("bronze_load_ts", current_timestamp())


# Write transformed data to the Delta table in oms_analytics_01_bronze
customers_transformed_df.write.format("delta").mode("overwrite").saveAsTable("oms_analytics.01_bronze.customers")

# Print end message with actual timestamp
print(f"Customer Silver Load Process completed at: {datetime.now()}")
