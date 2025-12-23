# Databricks notebook source
from pyspark.sql import functions as F

# Read Silver
SILVER_TABLE = "silver_servicenow_incidents"
GOLD_TABLE = "gold_ticket_counts_by_state_priority"

df_silver = spark.table(SILVER_TABLE)
print("Silver rows:", df_silver.count())


# COMMAND ----------

# Aggregate: count tickets by state and priority
df_gold = (
    df_silver
    .groupBy("state", "priority")
    .agg(F.count("*").alias("ticket_count"))
    .orderBy(F.col("ticket_count").desc())
)

display(df_gold)


# COMMAND ----------

# Save as Gold Delta table
(
    df_gold.write
      .format("delta")
      .mode("overwrite")
      .saveAsTable(GOLD_TABLE)
)

print(f"Gold Delta table created: {GOLD_TABLE}")
