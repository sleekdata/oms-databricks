# Databricks notebook source
# DBTITLE 1,Create Table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS oms_analytics.bronze.orders (
# MAGIC     path STRING,
# MAGIC     modificationTime TIMESTAMP,
# MAGIC     length LONG,
# MAGIC     content BINARY
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load Data
# MAGIC %sql
# MAGIC COPY INTO oms_analytics.bronze.orders
# MAGIC FROM 'abfss://orders@omslanding.dfs.core.windows.net/current'
# MAGIC FILEFORMAT = BINARYFILE;
