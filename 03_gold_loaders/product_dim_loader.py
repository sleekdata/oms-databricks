# Databricks notebook source
from datetime import datetime
from pyspark.sql.functions import current_timestamp, lit

# Print start message with actual timestamp
print(f"Product Gold Load Process started at: {datetime.now()}")

# Read data from the Silver tables
product_df = spark.read.table("oms_analytics.silver.product")
product_category_df = spark.read.table("oms_analytics.silver.product_category")
product_subcategory_df = spark.read.table("oms_analytics.silver.product_subcategory")

# Join the Silver tables
# Join product with product_subcategory on subcategory_id
product_with_subcategory_df = product_df.join(product_subcategory_df, on="product_subcat_id", how="inner")

# Join the result with product_category on category_id
product_gold_df = product_with_subcategory_df.join(product_category_df, on="category_id", how="inner")

# Transformations for Gold
product_gold_df = product_gold_df \
    .drop("silver_load_ts") \
    .drop("process_id") \
    .withColumn("process_id", lit("de_nb_102")) \
    .withColumn("gold_load_ts", current_timestamp())  # Set current timestamp for the gold_load_ts

# Select and reorder columns as per the Gold table schema
# Only include the necessary fields, remove product_desc and subcategory_desc if not present
columns_to_select = [
    "product_id",
    "product_name",
    "product_subcat_id",
    "subcategory_name",
    "category_id",
    "category_name",
    "category_desc",  # This field is available in product_category
    "process_id",
    "gold_load_ts"
]

# Write transformed data to the Delta table in oms_analytics.03_gold
product_gold_df = product_gold_df.select(*columns_to_select)

# Write to the Gold table
product_gold_df.write.format("delta").mode("overwrite").saveAsTable("oms_analytics.gold.product_dim")

# Print end message with actual timestamp
print(f"Product Gold Load Process completed at: {datetime.now()}")

