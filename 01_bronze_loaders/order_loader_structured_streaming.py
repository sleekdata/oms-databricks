# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import StructType, StructField, BinaryType, StringType, TimestampType, LongType


# Define the external location for Azure Data Lake Storage
external_location_name = "abfss://orders@omslanding.dfs.core.windows.net"

# Define the schema for the binary file (which contains metadata and binary content)
schema = StructType([
    StructField("path", StringType(), True),
    StructField("modificationTime", TimestampType(), True),
    StructField("length", LongType(), True),
    StructField("content", BinaryType(), True)
])

# Use Auto Loader to read the binary files in a streaming fashion from the external location
df = spark.readStream \
    .format("binaryFile") \
    .schema(schema) \
    .load(f"{external_location_name}/current")

# Transform the binary content into a string (assuming UTF-8 encoding), add extra columns
transformed_df = df \
    .withColumn("order_json_string", col("content").cast("string")) \
    .withColumn("process_id", lit("de_nb_102")) \
    .withColumn("bronze_load_ts", current_timestamp()) \
    .select("order_json_string", "process_id", "bronze_load_ts")

# Define the checkpoint location where the state will be saved for streaming
checkpoint_location = f"{external_location_name}/checkpoints/bornze_loader/orders"

# Write the transformed data to a Delta table, using append mode for continuous streaming data
transformed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_location) \
    .table("oms_analytics.bronze.orders")

# Start the streaming process and await termination
spark.streams.awaitAnyTermination()
