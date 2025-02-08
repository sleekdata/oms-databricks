# Databricks notebook source
# DBTITLE 1,Create Table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS oms_analytics.bronze.orders (
# MAGIC     order_id STRING,
# MAGIC     customer_id STRING,
# MAGIC     order_timestamp STRING,
# MAGIC     products ARRAY<STRUCT<product_id: STRING, quantity: STRING, unit_price: STRING>>
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load Data
# MAGIC %sql
# MAGIC COPY INTO oms_analytics.bronze.orders
# MAGIC FROM 'abfss://orders@omslanding.dfs.core.windows.net/current'
# MAGIC FILEFORMAT = JSON
# MAGIC FORMAT_OPTIONS ('inferSchema' = 'true', 'multiline' = 'true');
