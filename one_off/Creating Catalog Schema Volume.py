# Databricks notebook source


# COMMAND ----------

spark.sql("Create catalog if not exists nyctaxi")

# COMMAND ----------

spark.sql("create schema if not exists nyctaxi.00_landing")
spark.sql("create schema if not exists nyctaxi.01_bronze")
spark.sql("create schema if not exists nyctaxi.02_silver")
spark.sql("create schema if not exists nyctaxi.03_gold")

# COMMAND ----------

spark.sql("CREATE VOLUME IF NOT EXISTS nyctaxi.00_landing.data_sources")

# COMMAND ----------

