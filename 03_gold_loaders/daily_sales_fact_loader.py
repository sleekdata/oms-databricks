# Databricks notebook source
from datetime import datetime
from pyspark.sql.functions import current_timestamp, lit, col, abs, hash, sum
from pyspark.sql import functions as F

# Print start message with actual timestamp
print(f"Daily Sales Fact Load Process started at: {datetime.now()}")

# Read the entire historical data first (non-streaming)
orders_df = spark.table("oms_analytics.02_silver.orders")
order_items_df = spark.table("oms_analytics.02_silver.order_items")

# Process the entire data (non-streaming)
orders_df = orders_df.withWatermark("silver_load_ts", "5 minutes")
order_items_df = order_items_df.withWatermark("silver_load_ts", "5 minutes")

# Rename the silver_load_ts column in one of the DataFrames to avoid ambiguity
orders_df = orders_df.withColumnRenamed("silver_load_ts", "orders_silver_load_ts")
order_items_df = order_items_df.withColumnRenamed("silver_load_ts", "order_items_silver_load_ts")

# Join the orders and order_items DataFrames on order_item_id
joined_df = orders_df.join(order_items_df, on="order_item_id", how="inner")

# Calculate the surrogate_key as the hash of date_id, customer_id, and product_id
joined_df = joined_df.withColumn(
    "surrogate_key", abs(hash(col("date_id") + col("customer_id") + col("product_id"))))
    
# Aggregate the data to calculate items_sold and sales_amount
daily_sales_df = joined_df.groupBy(
    "surrogate_key",
    "date_id",
    "customer_id",
    "product_id",
    F.window("orders_silver_load_ts", "5 minutes")
).agg(
    sum("quantity").alias("items_sold"),  # Sum of quantity as items_sold
    sum("line_total").alias("sales_amount")  # Sum of line_total as sales_amount
)

# Add additional columns: process_id and gold_load_ts
daily_sales_df = daily_sales_df \
    .withColumn("process_id", lit("de_nb_102")) \
    .withColumn("gold_load_ts", current_timestamp())  # Set current timestamp for gold_load_ts

# Select final columns for the Gold table
final_columns = [
    "surrogate_key",
    "date_id",
    "customer_id",
    "product_id",
    "items_sold",
    "sales_amount",
    "process_id",
    "gold_load_ts"
]

# Final DataFrame selection
daily_sales_df = daily_sales_df.select(*final_columns)

# Define the external location for the Delta table
external_location_name = "abfss://orders@omslanding.dfs.core.windows.net"

# Define the checkpoint location where the state will be saved for streaming
checkpoint_location_daily_sales = f"{external_location_name}/checkpoints/gold_loader/daily_sales_fact"

# Write the batch data to the `daily_sales_fact` Gold table in Delta format
daily_sales_df.write.format("delta").mode("overwrite").saveAsTable("oms_analytics.03_gold.daily_sales_fact")

# Now, set up the streaming part
# Read streaming data from the Silver tables (this part will handle new data as it arrives)
orders_df = spark.readStream.table("oms_analytics.02_silver.orders")
order_items_df = spark.readStream.table("oms_analytics.02_silver.order_items")

# Add watermark again for the streaming data
orders_df = orders_df.withWatermark("silver_load_ts", "5 minutes")
order_items_df = order_items_df.withWatermark("silver_load_ts", "5 minutes")

# Rename the silver_load_ts column in one of the DataFrames to avoid ambiguity
orders_df = orders_df.withColumnRenamed("silver_load_ts", "orders_silver_load_ts")
order_items_df = order_items_df.withColumnRenamed("silver_load_ts", "order_items_silver_load_ts")

# Join the orders and order_items DataFrames on order_item_id for streaming data
joined_df = orders_df.join(order_items_df, on="order_item_id", how="inner")

# Calculate the surrogate_key as the hash of date_id, customer_id, and product_id
joined_df = joined_df.withColumn(
    "surrogate_key", abs(hash(col("date_id") + col("customer_id") + col("product_id"))))

# Aggregate the data for streaming
daily_sales_df = joined_df.groupBy(
    "surrogate_key",
    "date_id",
    "customer_id",
    "product_id",
    F.window("orders_silver_load_ts", "5 minutes")
).agg(
    sum("quantity").alias("items_sold"),
    sum("line_total").alias("sales_amount")
)

# Add additional columns: process_id and gold_load_ts
daily_sales_df = daily_sales_df \
    .withColumn("process_id", lit("de_nb_102")) \
    .withColumn("gold_load_ts", current_timestamp())  # Set current timestamp for gold_load_ts

# Final DataFrame selection for streaming
daily_sales_df = daily_sales_df.select(*final_columns)

# Write the streaming data to the `daily_sales_fact` Gold table in Delta format using streaming
daily_sales_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_location_daily_sales) \
    .table("oms_analytics.03_gold.daily_sales_fact")

# Start the streaming process and await termination
spark.streams.awaitAnyTermination()

