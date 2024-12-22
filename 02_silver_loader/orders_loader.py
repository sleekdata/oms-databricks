# Databricks notebook source
from pyspark.sql.functions import from_json, col, explode, lit, current_timestamp, to_date, expr, hash, abs
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from datetime import datetime

# Define the schema for the order JSON structure
order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_timestamp", StringType(), True),
    StructField("products", ArrayType(
        StructType([
            StructField("product_id", StringType(), True),
            StructField("quantity", StringType(), True),
            StructField("unit_price", StringType(), True)
        ])
    ), True)
])

# Function to get the current max order_item_id from the existing table
def get_max_order_item_id():
    try:
        # Try to get the max order_item_id from the existing table
        max_id = spark.table("oms_analytics.02_silver.order_items") \
                     .agg(expr("max(cast(order_item_id as bigint))").alias("max_order_item_id")) \
                     .collect()[0]["max_order_item_id"]
        return max_id if max_id is not None else 0
    except Exception as e:
        return 0

# Read and parse the JSON string from the Bronze table using spark.readStream.table
orders_df = spark.readStream.table("oms_analytics.01_bronze.orders") \
    .withColumn("order_json", from_json(col("order_json_string"), order_schema)) \
    .select("order_json.*")

# Get the current max order_item_id from the existing order_items table
max_order_item_id = get_max_order_item_id()

# Flatten the products array and create a separate row for each order item
orders_with_items_df = orders_df \
    .withColumn("products", explode(col("products"))) \
    .withColumn("order_item_id", abs(hash(col("order_id"), col("products.product_id")))) \
    .withColumn("product_id", col("products.product_id")) \
    .withColumn("quantity", col("products.quantity")) \
    .withColumn("unit_price", col("products.unit_price")) \
    .withColumn("line_total", col("quantity") * col("unit_price")) \
    .withColumn("process_id", lit("de_nb_102")) \
    .withColumn("silver_load_ts", current_timestamp()) \
    .drop("products")

# Create the `orders` table - one row per order
orders_silver_df = orders_with_items_df \
    .withColumn("order_id", col("order_id")) \
    .withColumn("customer_id", col("customer_id")) \
    .withColumn("order_timestamp", col("order_timestamp")) \
    .withColumn("date_id", to_date(col("order_timestamp")).cast("string")) \
    .select(
        "order_id", "customer_id", "date_id", "order_item_id", "order_timestamp",
        "process_id", "silver_load_ts"
    )


# Create the `order_items` table - one row per product/item
order_items_silver_df = orders_with_items_df \
    .select(
        "order_item_id", "order_id", "product_id", "quantity", "unit_price",
        "line_total", "process_id", "silver_load_ts"
    ) \
    .withColumn("process_id", lit("de_nb_102")) \
    .withColumn("silver_load_ts", current_timestamp())


# Define the external location for Azure Data Lake Storage
external_location_name = "abfss://orders@omslanding.dfs.core.windows.net"

# Define the checkpoint location where the state will be saved for streaming
checkpoint_location_orders = f"{external_location_name}/checkpoints/silver_loader/orders"
checkpoint_location_order_items = f"{external_location_name}/checkpoints/silver_loader/order_items"

# Write to the `orders` Silver table
orders_silver_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_location_orders) \
    .table("oms_analytics.02_silver.orders")

# Write to the `order_items` Silver table
order_items_silver_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_location_order_items) \
    .table("oms_analytics.02_silver.order_items")


# Start the streaming process and await termination
spark.streams.awaitAnyTermination()
