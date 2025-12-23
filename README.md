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
In Databricks (Free Edition):
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
