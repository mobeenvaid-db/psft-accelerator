# Databricks notebook source
# MAGIC %md
# MAGIC # PeopleSoft HCM Genie Room Accelerator
# MAGIC
# MAGIC **Turn your PeopleSoft HCM data into a natural language analytics experience in under an hour.**
# MAGIC
# MAGIC This accelerator creates a fully curated [Databricks Genie Room](https://docs.databricks.com/en/genie/index.html) on top of your existing PeopleSoft HCM tables — no data migration, no ETL changes, no new tables. Your HR team asks questions in plain English; Genie answers with governed, consistent metrics.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What You Get
# MAGIC
# MAGIC | Layer | Objects | Purpose |
# MAGIC |-------|---------|---------|
# MAGIC | **UC Metric Views** | `mv_headcount_workforce`, `mv_compensation`, `mv_training_compliance`, `mv_benefits` | Governed KPI definitions with semantic metadata. 25+ measures, 40+ dimensions, all with natural language synonyms so Genie maps "how many people" to `headcount`, "avg salary" to `avg_annual_salary`, etc. |
# MAGIC | **Denormalized Views** | `vw_employee_roster`, `vw_benefits_summary`, `vw_leave_balances`, `vw_training_compliance` | Flatten PeopleSoft's normalized schema into Genie-friendly shapes. Join 10+ PS tables into single wide views with human-readable labels and derived fields (tenure bands, age bands, clinical flags). |
# MAGIC | **Genie Room Config** | Instructions, 22 sample questions, validation queries | Copy-paste configuration for a production-ready Genie Room. |
# MAGIC
# MAGIC ## What You Don't Get (On Purpose)
# MAGIC
# MAGIC - **No new tables.** Views read directly from your existing PeopleSoft tables at query time.
# MAGIC - **No data movement.** Your PS data stays where it is — in your catalog, under your governance.
# MAGIC - **No hardcoded org logic.** SERVICE_LINE and CLINICAL_FLAG are marked as customization points with clear instructions.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC | Requirement | Details |
# MAGIC |-------------|---------|
# MAGIC | **PeopleSoft data in Databricks** | Via Lakeflow Connect, ETL pipeline, or Unity Catalog federation. Any method works — we just need the tables queryable in UC. |
# MAGIC | **Core PS tables accessible** | `PS_JOB`, `PS_PERSON`, `PS_NAMES`, `PS_PERS_DATA_EFFDT`, `PS_EMPLOYMENT`, `PS_DEPT_TBL`, `PS_LOCATION_TBL`, `PS_JOBCODE_TBL` |
# MAGIC | **Databricks Runtime 17.2+** | Required for UC Metric Views v1.1 (semantic metadata / synonyms). If on an older runtime, metric views will fall back to v0.1 without synonyms. |
# MAGIC | **SQL Warehouse** | For Genie Room queries. Serverless recommended. |
# MAGIC | **CREATE VIEW permissions** | On the target catalog/schema where views will be created. |
# MAGIC
# MAGIC ### Optional Tables (more analytics if present)
# MAGIC
# MAGIC | Table | What It Enables |
# MAGIC |-------|----------------|
# MAGIC | `PS_DIVERS_ETHNIC` | Ethnicity/race breakdowns, EEO reporting |
# MAGIC | `PS_HEALTH_BENEFIT` | Medical/dental/vision enrollment analytics |
# MAGIC | `PS_SAVINGS_PLAN` | 401k participation and contribution analysis |
# MAGIC | `PS_LEAVE_ACCRUAL` | PTO/sick balance tracking |
# MAGIC | `PS_TRAINING` | Compliance training completion rates |
# MAGIC | `PS_COMPENSATION` | Multi-component pay analysis |
# MAGIC | `PS_EMAIL_ADDRESSES` | Employee contact info in roster |
# MAGIC | `TL_RPTD_TIME` / `TL_PAYABLE_TIME` | Time and labor analytics |
# MAGIC
# MAGIC If any optional table is missing, the accelerator skips the related view/metric — everything else still works.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Setup (4 Steps, ~45 minutes)
# MAGIC
# MAGIC ### Step 1: Configure — `01_configure`
# MAGIC
# MAGIC **What it does:** Scans your source schema, finds your PeopleSoft tables, and maps them to standard names.
# MAGIC
# MAGIC **Parameters to set:**
# MAGIC | Parameter | Example | Description |
# MAGIC |-----------|---------|-------------|
# MAGIC | `source_catalog` | `production` | Catalog where your PeopleSoft tables live |
# MAGIC | `source_schema` | `peoplesoft_hcm` | Schema where your PeopleSoft tables live |
# MAGIC | `target_catalog` | `production` | Catalog for the new views (can be the same) |
# MAGIC | `target_schema` | `hcm_analytics` | Schema for views and metric views (will be created) |
# MAGIC
# MAGIC **What to look for:**
# MAGIC - Green `[FOUND]` for all 8 required tables
# MAGIC - If a table shows `[MISSING]`, check if it exists under a different name and add a manual override:
# MAGIC   ```python
# MAGIC   found["ps_names"] = "production.peoplesoft_hcm.EMPLOYEE_NAMES"
# MAGIC   ```
# MAGIC
# MAGIC **Time:** ~2 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 2: Build Views — `02_build_views`
# MAGIC
# MAGIC **What it does:** Creates denormalized views on top of your PS tables. No data is copied.
# MAGIC
# MAGIC **What to customize (2 fields in `vw_employee_roster`):**
# MAGIC
# MAGIC 1. **`CLINICAL_FLAG`** — Maps your JOB_FAMILY values to Clinical vs Non-Clinical.
# MAGIC    Default recognizes common PeopleSoft job family codes. Edit the `IN (...)` list to match yours:
# MAGIC    ```sql
# MAGIC    CASE WHEN jc.JOB_FAMILY IN ('NRS','THERAPY','AIDE') THEN 'Clinical' ELSE 'Non-Clinical' END
# MAGIC    ```
# MAGIC
# MAGIC 2. **`SERVICE_LINE`** — Maps your departments to business segments. Defaults to `'Unassigned'`.
# MAGIC    Replace with your department-to-service-line mapping:
# MAGIC    ```sql
# MAGIC    CASE
# MAGIC      WHEN j.DEPTID BETWEEN '100' AND '199' THEN 'Home Health'
# MAGIC      WHEN j.DEPTID BETWEEN '200' AND '299' THEN 'Hospice'
# MAGIC      ELSE 'Corporate'
# MAGIC    END
# MAGIC    ```
# MAGIC
# MAGIC **Time:** ~5 minutes (mostly customizing the two CASE statements)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 3: Build Metric Views — `03_build_metric_views`
# MAGIC
# MAGIC **What it does:** Creates UC Metric Views with governed KPI definitions and semantic metadata.
# MAGIC
# MAGIC **What you get:**
# MAGIC
# MAGIC | Metric View | Measures | Dimensions | Example Questions It Answers |
# MAGIC |-------------|----------|------------|------------------------------|
# MAGIC | `mv_headcount_workforce` | 9 (headcount, tenure, clinical %, FT %, terms, LOA) | 18 (dept, location, state, job family, tenure band, age band, gender, ethnicity, ...) | "How many active employees?", "Headcount by state", "What % is clinical?" |
# MAGIC | `mv_compensation` | 7 (avg/median/min/max salary, total payroll, hourly rate) | 12 (dept, job family, grade, gender, ethnicity, ...) | "Average salary by job family", "Pay gap by gender", "Total labor cost" |
# MAGIC | `mv_training_compliance` | 5 (assigned, completed, overdue, compliance rate) | 6 (course, status, dept, clinical flag) | "Compliance rate by course", "Which training is overdue?" |
# MAGIC | `mv_benefits` | 4 (enrolled count, avg deduction, annual cost) | 5 (benefit type, plan, coverage level, dept) | "Medical enrollment rate", "Avg annual benefit cost" |
# MAGIC
# MAGIC Each dimension includes **synonyms** — alternative phrases that help Genie map natural language to the right field:
# MAGIC - "how many people" → `headcount`
# MAGIC - "avg salary" → `avg_annual_salary`
# MAGIC - "turnover" → `terminated_count`
# MAGIC - "dept" → `department`
# MAGIC
# MAGIC **Time:** ~3 minutes (just run it)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 4: Create the Genie Room — `04_genie_room_setup`
# MAGIC
# MAGIC **What it does:** Provides everything you need to configure the Genie Room in the UI.
# MAGIC
# MAGIC **Steps:**
# MAGIC 1. Go to **Genie** in the left sidebar → **New** → **Genie space**
# MAGIC 2. Name it: `PeopleSoft HCM Analytics`
# MAGIC 3. Select your SQL warehouse
# MAGIC 4. **Add data sources** (Configure > Data > Add):
# MAGIC    - `mv_headcount_workforce` (metric view)
# MAGIC    - `mv_compensation` (metric view)
# MAGIC    - `mv_training_compliance` (metric view, if created)
# MAGIC    - `mv_benefits` (metric view, if created)
# MAGIC    - `vw_employee_roster` (view, for detailed queries)
# MAGIC 5. **Paste instructions** from `config/genie_instructions` into Configure > Instructions > Text
# MAGIC 6. **Add sample questions** from `config/sample_questions` into Configure > Settings
# MAGIC 7. Run the **validation queries** at the bottom of the notebook to confirm everything works
# MAGIC
# MAGIC **Time:** ~10 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## PeopleSoft Schema Reference
# MAGIC
# MAGIC The accelerator is validated against the **Oracle PeopleSoft HCM 9.2** delivered schema. Here's how the views map to your PS tables:
# MAGIC
# MAGIC ```
# MAGIC vw_employee_roster
# MAGIC  ├── PS_JOB .................. EMPLID, EMPL_RCD, EFFDT, DEPTID, JOBCODE, LOCATION,
# MAGIC  │                            EMPL_STATUS, ANNUAL_RT, HOURLY_RT, FLSA_STATUS
# MAGIC  ├── PS_PERSON ............... EMPLID, BIRTHDATE
# MAGIC  ├── PS_NAMES ................ EMPLID, NAME_TYPE, FIRST_NAME, LAST_NAME
# MAGIC  ├── PS_PERS_DATA_EFFDT ...... EMPLID, EFFDT, SEX, MAR_STATUS
# MAGIC  ├── PS_EMPLOYMENT ........... EMPLID, EMPL_RCD, HIRE_DT, TERMINATION_DT
# MAGIC  ├── PS_DIVERS_ETHNIC ........ EMPLID, ETHNIC_GRP_CD (optional)
# MAGIC  ├── PS_DEPT_TBL ............. DEPTID, DESCR, MANAGER_ID
# MAGIC  ├── PS_LOCATION_TBL ......... LOCATION, DESCR, CITY, STATE
# MAGIC  ├── PS_JOBCODE_TBL .......... JOBCODE, DESCR, JOB_FAMILY, FLSA_STATUS
# MAGIC  └── PS_EMAIL_ADDRESSES ...... EMPLID, EMAIL_ADDR (optional)
# MAGIC
# MAGIC vw_benefits_summary
# MAGIC  ├── PS_HEALTH_BENEFIT ....... EMPLID, PLAN_TYPE, COVRG_CD, DED_CUR
# MAGIC  └── vw_employee_roster ...... (for name, dept, job context)
# MAGIC
# MAGIC vw_leave_balances
# MAGIC  ├── PS_LEAVE_ACCRUAL ........ EMPLID, HRS_EARNED_YTD, HRS_TAKEN_YTD
# MAGIC  └── vw_employee_roster
# MAGIC
# MAGIC vw_training_compliance
# MAGIC  ├── PS_TRAINING ............. EMPLID, COURSE, ATTENDANCE, TRAINING_REASON
# MAGIC  └── vw_employee_roster
# MAGIC ```
# MAGIC
# MAGIC ### Key PeopleSoft Conventions Used
# MAGIC
# MAGIC | Convention | How We Handle It |
# MAGIC |-----------|-----------------|
# MAGIC | **Effective dating** (EFFDT/EFFSEQ) | Views filter to `EFFSEQ = 0` for current record. For historical analysis, query `ps_job` directly. |
# MAGIC | **SetID** | Table discovery handles SetID-partitioned tables. Views join on the key fields directly. |
# MAGIC | **EMPL_RCD** (multiple jobs) | Views join on `EMPL_RCD` to support concurrent employment records. |
# MAGIC | **Person model split** (9.2+) | Uses `PS_PERSON` + `PS_PERS_DATA_EFFDT` + `PS_NAMES` (not legacy `PS_PERSONAL_DATA`). |
# MAGIC | **PS_NAMES** (not PS_PERSON_NAME) | Auto-discovery checks both names. |
# MAGIC | **TL_QUANTITY** (not QUANTITY) | Correct PeopleSoft field name used in time views. |
# MAGIC | **PAYABLE_STATUS** on TL_PAYABLE_TIME | Not on TL_RPTD_TIME — correctly separated. |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Frequently Asked Questions
# MAGIC
# MAGIC **Q: We use PeopleSoft 8.x, not 9.2. Will this work?**
# MAGIC A: Mostly. The main difference is the Person model — 8.x uses `PS_PERSONAL_DATA` instead of `PS_PERSON` + `PS_PERS_DATA_EFFDT` + `PS_NAMES`. Auto-discovery checks for both. You may need manual overrides in `01_configure`.
# MAGIC
# MAGIC **Q: Our tables have custom prefixes (e.g., `XX_PS_JOB`).**
# MAGIC A: Use the manual override cell in `01_configure`:
# MAGIC ```python
# MAGIC found["ps_job"] = "my_catalog.my_schema.XX_PS_JOB"
# MAGIC ```
# MAGIC
# MAGIC **Q: We use Enterprise Learning Management (ELM), not legacy PS_TRAINING.**
# MAGIC A: Override the training table mapping to point to `PS_LM_CI_LRNHIST` or your ELM history table. The view will need column name adjustments — the ATTENDANCE and TRAINING_REASON fields may differ.
# MAGIC
# MAGIC **Q: Can we add more metrics later?**
# MAGIC A: Yes. Metric views are just SQL — `ALTER VIEW mv_headcount_workforce AS $$ ... $$` to add dimensions or measures. You can also create additional metric views for new domains.
# MAGIC
# MAGIC **Q: How do we refresh the data?**
# MAGIC A: You don't need to refresh anything on the Databricks side. The views query your PS tables at runtime. When your ETL updates the source tables, the views automatically reflect the new data.
# MAGIC
# MAGIC **Q: What if we want to track turnover trends over time?**
# MAGIC A: Create a monthly snapshot table from `vw_employee_roster` (scheduled job that writes headcount by dept/month). The accelerator includes a `turnover_monthly` pattern you can adopt.
# MAGIC
# MAGIC **Q: Does this work with federated catalogs (Foreign Catalog)?**
# MAGIC A: Yes. If your PeopleSoft data is in Oracle and accessed via a Unity Catalog foreign catalog, just point `source_catalog` and `source_schema` to the foreign catalog path. Views will query through federation.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                        GENIE ROOM                              │
# MAGIC │  "How many active employees by department?"                    │
# MAGIC │  "What's our average salary for nurses?"                       │
# MAGIC │  "Show me compliance training completion rates"                │
# MAGIC └──────────────────────────┬──────────────────────────────────────┘
# MAGIC                            │
# MAGIC              ┌─────────────▼─────────────┐
# MAGIC              │     UC METRIC VIEWS        │
# MAGIC              │  mv_headcount_workforce    │
# MAGIC              │  mv_compensation           │
# MAGIC              │  mv_training_compliance    │
# MAGIC              │  mv_benefits               │
# MAGIC              │                            │
# MAGIC              │  Governed KPIs + Synonyms  │
# MAGIC              └─────────────┬──────────────┘
# MAGIC                            │
# MAGIC              ┌─────────────▼─────────────┐
# MAGIC              │   DENORMALIZED VIEWS       │
# MAGIC              │  vw_employee_roster        │
# MAGIC              │  vw_benefits_summary       │
# MAGIC              │  vw_leave_balances         │
# MAGIC              │  vw_training_compliance    │
# MAGIC              │                            │
# MAGIC              │  Joins + Labels + Derived  │
# MAGIC              └─────────────┬──────────────┘
# MAGIC                            │
# MAGIC              ┌─────────────▼─────────────┐
# MAGIC              │  YOUR PEOPLESOFT TABLES    │
# MAGIC              │  (no changes needed)       │
# MAGIC              │                            │
# MAGIC              │  PS_JOB  PS_PERSON         │
# MAGIC              │  PS_NAMES  PS_EMPLOYMENT   │
# MAGIC              │  PS_DEPT_TBL  PS_LOCATION  │
# MAGIC              │  PS_JOBCODE_TBL  ...       │
# MAGIC              └────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Support
# MAGIC
# MAGIC Built by Databricks Field Engineering. For questions, customization help, or to request
# MAGIC additional PeopleSoft modules (Financials, Supply Chain, CRM), contact your Databricks SA.
