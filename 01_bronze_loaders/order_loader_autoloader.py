# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit, col

# Define the external location for Azure Data Lake Storage
external_location_name = "abfss://orders@omslanding.dfs.core.windows.net"

# Define the checkpoint location
checkpoint_location = f"{external_location_name}/checkpoints/bronze_loader/orders"

# Use Auto Loader to read the JSON files in a streaming fashion from the external location
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "binaryFile") \
    .option("cloudFiles.inferSchema", "true") \
    .option("cloudFiles.schemaLocation", checkpoint_location) \
    .load(f"{external_location_name}/current")

# Transform the data as needed
transformed_df = df \
    .withColumn("process_id", lit("de_nb_102")) \
    .withColumn("bronze_load_ts", current_timestamp())  \
    .withColumnRenamed("content", "order_json")

# Write the transformed data to a Delta table
transformed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_location) \
    .table("oms_analytics.bronze.orders")

# Start the streaming process and await termination
spark.streams.awaitAnyTermination()
