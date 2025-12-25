# Medallion-Pipeline
This project implements a simple Medallion Architecture pipeline in Databricks using a ServiceNow incidents CSV dataset (`servicenow_incidents_10k.csv`).   

Goal: show how I ingest raw data, clean/transform it, and produce an aggregated Gold dataset for analytics.

---
## Contents
- `01_bronze_ingest` notebook
- `02_silver_clean_transform` notebook
- `03_gold_analytics` notebook
- Input: `servicenow_incidents_10k.csv`

---
## How to run the pipeline

### 0) Upload data
In Databricks:
1. **New → File (Add data)**
2. Upload `servicenow_incidents_10k.csv`
3. Choose **Create Table**
4. Note the created source table name (example): `workspace.default.servicenow_incidents_10_k`

> Note: I used “Create Table” because reading directly from Workspace Files caused compute read errors in this environment.

---

### 1) Run Bronze
Open notebook: **`01_bronze_ingest`**
- Reads the uploaded source table
- Writes raw data to a Delta Bronze table
- Adds ingestion metadata columns (`_ingest_ts`, `_source_file`)
- Validates row counts

Output:
- Delta table: `bronze_servicenow_incidents`

---

### 2) Run Silver
Open notebook: **`02_silver_clean_transform`**
- Normalizes column names (safe/consistent naming)
- Removes invalid records (missing/blank `sys_id`)
- Converts date strings (`opened_at`, `closed_at`) into timestamps (`opened_at_ts`, `closed_at_ts`)
  - Uses tolerant parsing (`try_to_timestamp`) + multiple patterns to handle inconsistent formats
- Writes cleaned data to a Delta Silver table
- Validates key constraints and parsing success

Output:
- Delta table: `silver_servicenow_incidents`

---

### 3) Run Gold
Open notebook: **`03_gold_analytics`**
- Aggregates ticket counts by status and priority
  - In this dataset, `state` is used as ticket status
- Writes Gold output as a Delta table

Output:
- Delta table: `gold_ticket_counts_by_state_priority`

---


## Data model / tables

### Bronze: `bronze_servicenow_incidents`
- Raw landing zone
- Preserves source fields as-is
- Adds ingestion metadata columns:
  - `_ingest_ts`
  - `_source_file`

### Silver: `silver_servicenow_incidents`
- Cleaned + standardized
- Key rule:
  - Filter out invalid records where `sys_id` is null/empty
- Timestamp parsing:
  - `opened_at_ts`, `closed_at_ts` created from string fields
  - Tolerant parsing handles mixed formats like:
    - `6/5/2025 13:10`
    - `2/15/2025 3:21`

### Gold: `gold_ticket_counts_by_state_priority`
- Aggregated analytics-ready table
- Metric:
  - `ticket_count` grouped by `state` (status) and `priority`

---

## Design decisions (and why)

### 1) Why Medallion Architecture
- **Bronze** = raw, replayable source of truth
- **Silver** = enforce data quality + correct types
- **Gold** = business-ready aggregates for reporting

This separation makes the pipeline easier to debug, safer to evolve, and more production-aligned.

### 2) Preserve schema in Bronze
Bronze does not rename or clean business columns.  
Only ingestion metadata is added (standard practice for lineage and debugging).

### 3) Data quality rule (invalid records)
I treated `sys_id` as the primary identifier for ServiceNow incidents and removed records where it is missing/blank.  
This prevents unreliable joins and incorrect aggregations downstream.

### 4) Timestamp parsing strategy
The dataset contained inconsistent timestamp formats. A strict single-format parse caused runtime failures.  
In Silver, I used tolerant parsing with multiple patterns to avoid failing the pipeline and to keep the system robust.

### 5) Databricks Free Edition constraints
Direct Spark reads from Workspace Files/DBFS paths were restricted in this environment (Public DBFS root disabled / read errors).  
To make ingestion reliable, I used Databricks **Add Data → Create Table** and then read from that table.

---
## How to validate outputs quickly

Run these in a notebook:

```python
# Bronze
spark.table("bronze_servicenow_incidents").count()
spark.table("bronze_servicenow_incidents").printSchema()

# Silver
spark.table("silver_servicenow_incidents").count()
spark.table("silver_servicenow_incidents").printSchema()

# Gold
display(spark.table("gold_ticket_counts_by_state_priority").orderBy("ticket_count", ascending=False))

---

## Tech used

1. Databricks (Spark / PySpark)

2. Delta Lake tables (Bronze/Silver/Gold)


