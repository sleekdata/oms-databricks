# Databricks notebook source
# Imports
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

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

# Read and parse the JSON string from the Bronze table
orders_with_items_df = spark.readStream.table("oms_analytics.bronze.orders") \
    .withColumn("order_json", F.from_json(F.col("order_json").cast("string"), order_schema)) \
    .select("order_json.*")

# Flatten the products array and create a separate row for each order item
orders_with_items_df = orders_with_items_df \
    .withColumn("products", F.explode(F.col("products"))) \
    .withColumn("order_item_id", F.abs(F.hash(F.col("order_id"), F.col("products.product_id")))) \
    .withColumn("product_id", F.col("products.product_id")) \
    .withColumn("quantity", F.col("products.quantity")) \
    .withColumn("unit_price", F.col("products.unit_price")) \
    .withColumn("line_total", F.col("quantity") * F.col("unit_price")) \
    .withColumn("order_timestamp", F.to_timestamp(F.col("order_timestamp"))) \
    .withColumn("process_id", F.lit("de_nb_102")) \
    .withColumn("silver_load_ts", F.current_timestamp()) \
    .drop("products")

# Create the `orders` table - one row per order
orders_silver_df = orders_with_items_df \
    .withColumn("date_id", F.to_date(F.col("order_timestamp")).cast("string")) \
    .select(
        "order_id", "customer_id", "date_id", "order_timestamp",
        "process_id", "silver_load_ts"
    ).distinct()
    
# Create the `order_items` table - one row per product/item (added `order_timestamp`)
order_items_silver_df = orders_with_items_df \
    .select(
        "order_item_id", "order_id", "product_id", "quantity", "unit_price",
        "line_total", "order_timestamp", "process_id", "silver_load_ts"
    )

# Define the external location for Azure Data Lake Storage
external_location_name = "abfss://orders@omslanding.dfs.core.windows.net"

# Define the checkpoint locations
checkpoint_location_orders = f"{external_location_name}/checkpoints/silver_loader/orders"
checkpoint_location_order_items = f"{external_location_name}/checkpoints/silver_loader/order_items"


# Write to the `orders` Silver table
orders_silver_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_location_orders) \
    .table("oms_analytics.silver.orders")

# Write to the `order_items` Silver table (includes `order_timestamp`)
order_items_silver_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_location_order_items) \
    .table("oms_analytics.silver.order_items")

# Start the streaming process and await termination
spark.streams.awaitAnyTermination()
