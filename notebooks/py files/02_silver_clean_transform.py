# Databricks notebook source
# 02_silver_clean_transform
# Goal:
# 1) Clean and transform:
# 2) Normalize column names.
# 3) Remove invalid records (e.g., missing ticket_id).
# 4) Convert date fields to proper timestamp format.
# 5) Save as Delta table or parquet.

from pyspark.sql import functions as F

# Read from the bronze table
BRONZE_TABLE = "bronze_servicenow_incidents"
SILVER_TABLE = "silver_servicenow_incidents"

df_bronze = spark.table(BRONZE_TABLE)

print("Bronze rows:", df_bronze.count())
df_bronze.printSchema()


# COMMAND ----------

# Normalize column names
def normalize_columns(df):
    for col in df.columns:
        normalized = (
            col.lower()
               .replace(" ", "_")
               .replace("-", "_")
        )
        df = df.withColumnRenamed(col, normalized)
    return df

df_norm = normalize_columns(df_bronze)


# COMMAND ----------

# Remove invalid records (missing ticket id)
df_valid = (
    df_norm
    .filter(
        F.col("sys_id").isNotNull() &
        (F.trim(F.col("sys_id")) != "")
    )
)

print("After removing invalid records:", df_valid.count())


# COMMAND ----------

# Convert date fields to proper timestamp format.
df_dates = (
    df_valid
    .withColumn("opened_at_ts", F.to_timestamp("opened_at"))
    .withColumn("closed_at_ts", F.to_timestamp("closed_at"))
)


# COMMAND ----------

# Write Silver Delta table
(
    df_dates.write
      .format("delta")
      .mode("overwrite")
      .saveAsTable(SILVER_TABLE)
)

print(f"Silver Delta table created: {SILVER_TABLE}")


# COMMAND ----------

# Occured in "TIMESTAMP" error. 
# The proper time format should be '2025-06-05 13:10:00' 
df_dates = (
    df_valid
    .withColumn(
        "opened_at_ts",
        F.to_timestamp("opened_at", "M/d/yyyy HH:mm")
    )
    .withColumn(
        "closed_at_ts",
        F.to_timestamp("closed_at", "M/d/yyyy HH:mm")
    )
)


# COMMAND ----------

# Write Silver Delta table again
(
    df_dates.write
      .format("delta")
      .mode("overwrite")
      .saveAsTable(SILVER_TABLE)
)

print(f"Silver Delta table created: {SILVER_TABLE}")

# COMMAND ----------

# Try to convert date fields using 'try_to_timestamp'
df_dates = (
    df_valid
    .withColumn(
        "opened_at_ts",
        F.coalesce(
            F.expr("try_to_timestamp(opened_at, 'M/d/yyyy HH:mm')"),
            F.expr("try_to_timestamp(opened_at, 'M/d/yyyy H:mm')")
        )
    )
    .withColumn(
        "closed_at_ts",
        F.coalesce(
            F.expr("try_to_timestamp(closed_at, 'M/d/yyyy HH:mm')"),
            F.expr("try_to_timestamp(closed_at, 'M/d/yyyy H:mm')")
        )
    )
)


# COMMAND ----------

# Write Silver Delta table
(
    df_dates.write
      .format("delta")
      .mode("overwrite")
      .saveAsTable(SILVER_TABLE)
)

print(f"Silver Delta table created: {SILVER_TABLE}")

# COMMAND ----------

# Confirm Silver table.
# Shows sample rows.
spark.table("silver_servicenow_incidents").printSchema()
display(spark.table("silver_servicenow_incidents").limit(20))

# Row count validation.
print("Bronze rows:", spark.table("bronze_servicenow_incidents").count())
print("Silver rows:", spark.table("silver_servicenow_incidents").count())