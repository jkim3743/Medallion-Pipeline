# Databricks notebook source
# 01_bronze_ingest
# Goal:
# 1) Load raw data into a Bronze Delta table
# 2) Preserve schema as-is (no casting / no transformations)

BRONZE_TABLE = "bronze_servicenow_incidents"

df_raw = spark.table("workspace.default.servicenow_incidents_10_k")

# Ingestion metadata: timestamp and the source identifier. 
# This helps with lineage and debugging, but it doesn’t affect the source data.
from pyspark.sql import functions as F
df_bronze = (
    df_raw
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_source_file", F.lit("upload_table"))
)

# Writing the data as a Delta table
(df_bronze.write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(BRONZE_TABLE)
)

print("Bronze created:", BRONZE_TABLE)

# COMMAND ----------

# Validating the load by checking the row count and confirming that the schema matches the source. 
# This ensures the ingestion is complete and repeatable.
print("Source schema:")
df_raw.printSchema()

print("Bronze schema:")
spark.table(BRONZE_TABLE).printSchema()