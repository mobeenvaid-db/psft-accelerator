# Databricks notebook source
# MAGIC %md
# MAGIC # PeopleSoft HCM Genie Room Accelerator
# MAGIC ## Step 1: Configure Your Environment
# MAGIC
# MAGIC This notebook discovers your existing PeopleSoft tables and configures the accelerator
# MAGIC to work with your environment. No tables are created — we build views and metric views
# MAGIC on top of what you already have.
# MAGIC
# MAGIC ### What You Need
# MAGIC - PeopleSoft HCM data already in Databricks (via Lakeflow Connect, ETL, or federation)
# MAGIC - The following core PS tables accessible in Unity Catalog:
# MAGIC   - `PS_JOB` (or equivalent)
# MAGIC   - `PS_PERSON` or `PS_PERSONAL_DATA`
# MAGIC   - `PS_NAMES` (or `PS_PERSON_NAME` in older versions)
# MAGIC   - `PS_PERS_DATA_EFFDT` (or `PS_PERSONAL_DATA` if pre-9.2)
# MAGIC   - `PS_EMPLOYMENT`
# MAGIC   - `PS_DEPT_TBL`
# MAGIC   - `PS_LOCATION_TBL`
# MAGIC   - `PS_JOBCODE_TBL`
# MAGIC
# MAGIC ### Optional (for additional analytics)
# MAGIC   - `PS_DIVERS_ETHNIC` — ethnicity/EEO reporting
# MAGIC   - `PS_COMPENSATION` — multi-component pay
# MAGIC   - `PS_HEALTH_BENEFIT` — medical/dental/vision enrollment
# MAGIC   - `PS_SAVINGS_PLAN` — 401k enrollment
# MAGIC   - `PS_LEAVE_ACCRUAL` — PTO/leave balances
# MAGIC   - `PS_TRAINING` or ELM tables — training compliance
# MAGIC   - `PS_EMAIL_ADDRESSES` — employee email
# MAGIC   - `TL_RPTD_TIME` / `TL_PAYABLE_TIME` — time and labor

# COMMAND ----------

dbutils.widgets.text("source_catalog", "", "Catalog where your PS tables live")
dbutils.widgets.text("source_schema", "", "Schema where your PS tables live")
dbutils.widgets.text("target_catalog", "", "Catalog for views/metric views (can be same)")
dbutils.widgets.text("target_schema", "hcm_analytics", "Schema for views/metric views")

source_catalog = dbutils.widgets.get("source_catalog")
source_schema = dbutils.widgets.get("source_schema")
target_catalog = dbutils.widgets.get("target_catalog") or source_catalog
target_schema = dbutils.widgets.get("target_schema")

src = f"{source_catalog}.{source_schema}"
tgt = f"{target_catalog}.{target_schema}"

print(f"Source (your PS tables): {src}")
print(f"Target (views/metrics):  {tgt}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Discover Your PeopleSoft Tables
# MAGIC We'll scan your source schema to find which PS tables exist and map them.

# COMMAND ----------

# Standard PeopleSoft table names we look for
PS_TABLE_MAP = {
    # Core (required)
    "ps_job":              ["PS_JOB", "PSJOB", "PS_JOB_DATA", "JOB_DATA"],
    "ps_person":           ["PS_PERSON", "PS_PERSONAL_DATA", "PERSONAL_DATA"],
    "ps_names":            ["PS_NAMES", "PS_PERSON_NAME", "PERSON_NAME", "PS_NAME"],
    "ps_pers_data_effdt":  ["PS_PERS_DATA_EFFDT", "PERS_DATA_EFFDT", "PS_PERSONAL_DATA"],
    "ps_employment":       ["PS_EMPLOYMENT", "EMPLOYMENT", "PS_PER_ORG_INST"],
    "ps_dept_tbl":         ["PS_DEPT_TBL", "DEPT_TBL", "DEPARTMENT"],
    "ps_location_tbl":     ["PS_LOCATION_TBL", "LOCATION_TBL", "LOCATION"],
    "ps_jobcode_tbl":      ["PS_JOBCODE_TBL", "JOBCODE_TBL", "JOB_CODE"],
    # Optional
    "ps_divers_ethnic":    ["PS_DIVERS_ETHNIC", "DIVERS_ETHNIC", "DIVERSITY"],
    "ps_compensation":     ["PS_COMPENSATION", "COMPENSATION"],
    "ps_health_benefit":   ["PS_HEALTH_BENEFIT", "HEALTH_BENEFIT"],
    "ps_savings_plan":     ["PS_SAVINGS_PLAN", "SAVINGS_PLAN"],
    "ps_leave_accrual":    ["PS_LEAVE_ACCRUAL", "LEAVE_ACCRUAL"],
    "ps_training":         ["PS_TRAINING", "TRAINING", "PS_LM_CI_LRNHIST"],
    "ps_email_addresses":  ["PS_EMAIL_ADDRESSES", "EMAIL_ADDRESSES"],
    "ps_actn_reason_tbl":  ["PS_ACTN_REASON_TBL", "ACTN_REASON_TBL"],
    "tl_rptd_time":        ["TL_RPTD_TIME", "RPTD_TIME", "TIME_REPORTING"],
    "tl_payable_time":     ["TL_PAYABLE_TIME", "PAYABLE_TIME"],
}

# Get all tables in the source schema
existing_tables = [row.tableName.upper() for row in spark.sql(f"SHOW TABLES IN {src}").collect()]
print(f"Found {len(existing_tables)} tables in {src}\n")

# Map standard names to actual table names in their environment
found = {}
missing_required = []
missing_optional = []
required = {"ps_job", "ps_person", "ps_names", "ps_pers_data_effdt", "ps_employment",
            "ps_dept_tbl", "ps_location_tbl", "ps_jobcode_tbl"}

for standard_name, search_names in PS_TABLE_MAP.items():
    matched = None
    for candidate in search_names:
        if candidate.upper() in existing_tables:
            matched = candidate
            break

    if matched:
        found[standard_name] = f"{src}.{matched}"
        print(f"  [FOUND]   {standard_name:25s} -> {matched}")
    elif standard_name in required:
        missing_required.append(standard_name)
        print(f"  [MISSING] {standard_name:25s}  ** REQUIRED **")
    else:
        missing_optional.append(standard_name)
        print(f"  [MISSING] {standard_name:25s}  (optional)")

print(f"\nFound: {len(found)} / {len(PS_TABLE_MAP)}")
if missing_required:
    print(f"\n*** REQUIRED TABLES MISSING: {missing_required}")
    print("*** The accelerator needs these tables. Check if they exist under different names.")
    print("*** You can manually set the mapping below.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Manual Overrides
# MAGIC If auto-discovery missed a table (different naming convention), set it here:
# MAGIC ```python
# MAGIC found["ps_job"] = "my_catalog.my_schema.MY_CUSTOM_JOB_TABLE"
# MAGIC found["ps_names"] = "my_catalog.my_schema.EMPLOYEE_NAMES"
# MAGIC ```

# COMMAND ----------

# Save the mapping for downstream notebooks
import json

# Store config
config = {
    "source": src,
    "target": tgt,
    "table_map": found,
    "missing_required": missing_required,
    "missing_optional": missing_optional,
}

# Persist to spark conf so downstream notebooks can read it
spark.conf.set("accelerator.source", src)
spark.conf.set("accelerator.target", tgt)
spark.conf.set("accelerator.target_catalog", target_catalog)
spark.conf.set("accelerator.target_schema", target_schema)

for k, v in found.items():
    spark.conf.set(f"accelerator.table.{k}", v)

# Also save to a temp table for persistence across notebook runs
spark.createDataFrame(
    [(k, v) for k, v in found.items()],
    ["standard_name", "actual_table"]
).write.mode("overwrite").saveAsTable(f"{tgt}._accelerator_config")

print(f"\nConfiguration saved. {len(found)} tables mapped.")
print(f"Target schema for views: {tgt}")

# COMMAND ----------

# Create target schema if needed
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {tgt} COMMENT 'PeopleSoft HCM analytics layer - views and metric views for Genie Room'")
print(f"Target schema ready: {tgt}")
