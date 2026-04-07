# PeopleSoft HCM Genie Room Accelerator

**Turn your PeopleSoft HCM data into a natural language analytics experience in under an hour.**

This accelerator creates a fully curated [Databricks Genie Room](https://docs.databricks.com/en/genie/index.html) on top of your existing PeopleSoft HCM tables — no data migration, no ETL changes, no new tables. Your HR team asks questions in plain English; Genie answers with governed, consistent metrics.

---

## What You Get

| Layer | Objects | Purpose |
|-------|---------|---------|
| **UC Metric Views** | `mv_headcount_workforce`, `mv_compensation`, `mv_training_compliance`, `mv_benefits` | Governed KPI definitions with semantic metadata. 25+ measures, 40+ dimensions, all with natural language synonyms so Genie maps "how many people" to `headcount`, "avg salary" to `avg_annual_salary`, etc. |
| **Denormalized Views** | `vw_employee_roster`, `vw_benefits_summary`, `vw_leave_balances`, `vw_training_compliance` | Flatten PeopleSoft's normalized schema into Genie-friendly shapes. Join 10+ PS tables into single wide views with human-readable labels and derived fields (tenure bands, age bands, clinical flags). |
| **Genie Room Config** | Instructions, 22 sample questions, validation queries | Copy-paste configuration for a production-ready Genie Room. |

## What You Don't Get (On Purpose)

- **No new tables.** Views read directly from your existing PeopleSoft tables at query time.
- **No data movement.** Your PS data stays where it is — in your catalog, under your governance.
- **No hardcoded org logic.** SERVICE_LINE and CLINICAL_FLAG are marked as customization points with clear instructions.

---

## Prerequisites

| Requirement | Details |
|-------------|---------|
| **PeopleSoft data in Databricks** | Via Lakeflow Connect, ETL pipeline, or Unity Catalog federation. Any method works — we just need the tables queryable in UC. |
| **Core PS tables accessible** | `PS_JOB`, `PS_PERSON`, `PS_NAMES`, `PS_PERS_DATA_EFFDT`, `PS_EMPLOYMENT`, `PS_DEPT_TBL`, `PS_LOCATION_TBL`, `PS_JOBCODE_TBL` |
| **Databricks Runtime 17.2+** | Required for UC Metric Views v1.1 (semantic metadata / synonyms). If on an older runtime, metric views will fall back to v0.1 without synonyms. |
| **SQL Warehouse** | For Genie Room queries. Serverless recommended. |
| **CREATE VIEW permissions** | On the target catalog/schema where views will be created. |

### Optional Tables (more analytics if present)

| Table | What It Enables |
|-------|----------------|
| `PS_DIVERS_ETHNIC` | Ethnicity/race breakdowns, EEO reporting |
| `PS_HEALTH_BENEFIT` | Medical/dental/vision enrollment analytics |
| `PS_SAVINGS_PLAN` | 401k participation and contribution analysis |
| `PS_LEAVE_ACCRUAL` | PTO/sick balance tracking |
| `PS_TRAINING` | Compliance training completion rates |
| `PS_COMPENSATION` | Multi-component pay analysis |
| `PS_EMAIL_ADDRESSES` | Employee contact info in roster |
| `TL_RPTD_TIME` / `TL_PAYABLE_TIME` | Time and labor analytics |

If any optional table is missing, the accelerator skips the related view/metric — everything else still works.

---

## Setup (4 Steps, ~45 minutes)

### Step 1: Configure — `01_configure`

**What it does:** Scans your source schema, finds your PeopleSoft tables, and maps them to standard names.

**Parameters to set:**

| Parameter | Example | Description |
|-----------|---------|-------------|
| `source_catalog` | `production` | Catalog where your PeopleSoft tables live |
| `source_schema` | `peoplesoft_hcm` | Schema where your PeopleSoft tables live |
| `target_catalog` | `production` | Catalog for the new views (can be the same) |
| `target_schema` | `hcm_analytics` | Schema for views and metric views (will be created) |

**What to look for:**
- Green `[FOUND]` for all 8 required tables
- If a table shows `[MISSING]`, check if it exists under a different name and add a manual override:
  ```python
  found["ps_names"] = "production.peoplesoft_hcm.EMPLOYEE_NAMES"
  ```

**Time:** ~2 minutes

---

### Step 2: Build Views — `02_build_views`

**What it does:** Creates denormalized views on top of your PS tables. No data is copied.

**What to customize (2 fields in `vw_employee_roster`):**

1. **`CLINICAL_FLAG`** — Maps your JOB_FAMILY values to Clinical vs Non-Clinical.
   Default recognizes common PeopleSoft job family codes. Edit the `IN (...)` list to match yours:
   ```sql
   CASE WHEN jc.JOB_FAMILY IN ('NRS','THERAPY','AIDE') THEN 'Clinical' ELSE 'Non-Clinical' END
   ```

2. **`SERVICE_LINE`** — Maps your departments to business segments. Defaults to `'Unassigned'`.
   Replace with your department-to-service-line mapping:
   ```sql
   CASE
     WHEN j.DEPTID BETWEEN '100' AND '199' THEN 'Home Health'
     WHEN j.DEPTID BETWEEN '200' AND '299' THEN 'Hospice'
     ELSE 'Corporate'
   END
   ```

**Time:** ~5 minutes (mostly customizing the two CASE statements)

---

### Step 3: Build Metric Views — `03_build_metric_views`

**What it does:** Creates UC Metric Views with governed KPI definitions and semantic metadata.

**What you get:**

| Metric View | Measures | Dimensions | Example Questions It Answers |
|-------------|----------|------------|------------------------------|
| `mv_headcount_workforce` | 9 (headcount, tenure, clinical %, FT %, terms, LOA) | 18 (dept, location, state, job family, tenure band, age band, gender, ethnicity, ...) | "How many active employees?", "Headcount by state", "What % is clinical?" |
| `mv_compensation` | 7 (avg/median/min/max salary, total payroll, hourly rate) | 12 (dept, job family, grade, gender, ethnicity, ...) | "Average salary by job family", "Pay gap by gender", "Total labor cost" |
| `mv_training_compliance` | 5 (assigned, completed, overdue, compliance rate) | 6 (course, status, dept, clinical flag) | "Compliance rate by course", "Which training is overdue?" |
| `mv_benefits` | 4 (enrolled count, avg deduction, annual cost) | 5 (benefit type, plan, coverage level, dept) | "Medical enrollment rate", "Avg annual benefit cost" |

Each dimension includes **synonyms** — alternative phrases that help Genie map natural language to the right field:
- "how many people" → `headcount`
- "avg salary" → `avg_annual_salary`
- "turnover" → `terminated_count`
- "dept" → `department`

**Time:** ~3 minutes (just run it)

---

### Step 4: Create the Genie Room — `04_genie_room_setup`

**What it does:** Provides everything you need to configure the Genie Room in the UI.

**Steps:**
1. Go to **Genie** in the left sidebar → **New** → **Genie space**
2. Name it: `PeopleSoft HCM Analytics`
3. Select your SQL warehouse
4. **Add data sources** (Configure > Data > Add):
   - `mv_headcount_workforce` (metric view)
   - `mv_compensation` (metric view)
   - `mv_training_compliance` (metric view, if created)
   - `mv_benefits` (metric view, if created)
   - `vw_employee_roster` (view, for detailed queries)
5. **Paste instructions** from `config/genie_instructions` into Configure > Instructions > Text
6. **Add sample questions** from `config/sample_questions` into Configure > Settings
7. Run the **validation queries** at the bottom of the notebook to confirm everything works

**Time:** ~10 minutes

---

## PeopleSoft Schema Reference

The accelerator is validated against the **Oracle PeopleSoft HCM 9.2** delivered schema. Here's how the views map to your PS tables:

```
vw_employee_roster
 ├── PS_JOB .................. EMPLID, EMPL_RCD, EFFDT, DEPTID, JOBCODE, LOCATION,
 │                            EMPL_STATUS, ANNUAL_RT, HOURLY_RT, FLSA_STATUS
 ├── PS_PERSON ............... EMPLID, BIRTHDATE
 ├── PS_NAMES ................ EMPLID, NAME_TYPE, FIRST_NAME, LAST_NAME
 ├── PS_PERS_DATA_EFFDT ...... EMPLID, EFFDT, SEX, MAR_STATUS
 ├── PS_EMPLOYMENT ........... EMPLID, EMPL_RCD, HIRE_DT, TERMINATION_DT
 ├── PS_DIVERS_ETHNIC ........ EMPLID, ETHNIC_GRP_CD (optional)
 ├── PS_DEPT_TBL ............. DEPTID, DESCR, MANAGER_ID
 ├── PS_LOCATION_TBL ......... LOCATION, DESCR, CITY, STATE
 ├── PS_JOBCODE_TBL .......... JOBCODE, DESCR, JOB_FAMILY, FLSA_STATUS
 └── PS_EMAIL_ADDRESSES ...... EMPLID, EMAIL_ADDR (optional)

vw_benefits_summary
 ├── PS_HEALTH_BENEFIT ....... EMPLID, PLAN_TYPE, COVRG_CD, DED_CUR
 └── vw_employee_roster ...... (for name, dept, job context)

vw_leave_balances
 ├── PS_LEAVE_ACCRUAL ........ EMPLID, HRS_EARNED_YTD, HRS_TAKEN_YTD
 └── vw_employee_roster

vw_training_compliance
 ├── PS_TRAINING ............. EMPLID, COURSE, ATTENDANCE, TRAINING_REASON
 └── vw_employee_roster
```

### Key PeopleSoft Conventions Used

| Convention | How We Handle It |
|-----------|-----------------|
| **Effective dating** (EFFDT/EFFSEQ) | Views filter to `EFFSEQ = 0` for current record. For historical analysis, query `ps_job` directly. |
| **SetID** | Table discovery handles SetID-partitioned tables. Views join on the key fields directly. |
| **EMPL_RCD** (multiple jobs) | Views join on `EMPL_RCD` to support concurrent employment records. |
| **Person model split** (9.2+) | Uses `PS_PERSON` + `PS_PERS_DATA_EFFDT` + `PS_NAMES` (not legacy `PS_PERSONAL_DATA`). |
| **PS_NAMES** (not PS_PERSON_NAME) | Auto-discovery checks both names. |
| **TL_QUANTITY** (not QUANTITY) | Correct PeopleSoft field name used in time views. |
| **PAYABLE_STATUS** on TL_PAYABLE_TIME | Not on TL_RPTD_TIME — correctly separated. |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        GENIE ROOM                              │
│  "How many active employees by department?"                    │
│  "What's our average salary for nurses?"                       │
│  "Show me compliance training completion rates"                │
└──────────────────────────┬──────────────────────────────────────┘
                           │
             ┌─────────────▼─────────────┐
             │     UC METRIC VIEWS        │
             │  mv_headcount_workforce    │
             │  mv_compensation           │
             │  mv_training_compliance    │
             │  mv_benefits               │
             │                            │
             │  Governed KPIs + Synonyms  │
             └─────────────┬──────────────┘
                           │
             ┌─────────────▼─────────────┐
             │   DENORMALIZED VIEWS       │
             │  vw_employee_roster        │
             │  vw_benefits_summary       │
             │  vw_leave_balances         │
             │  vw_training_compliance    │
             │                            │
             │  Joins + Labels + Derived  │
             └─────────────┬──────────────┘
                           │
             ┌─────────────▼─────────────┐
             │  YOUR PEOPLESOFT TABLES    │
             │  (no changes needed)       │
             │                            │
             │  PS_JOB  PS_PERSON         │
             │  PS_NAMES  PS_EMPLOYMENT   │
             │  PS_DEPT_TBL  PS_LOCATION  │
             │  PS_JOBCODE_TBL  ...       │
             └────────────────────────────┘
```

---

## Frequently Asked Questions

**Q: We use PeopleSoft 8.x, not 9.2. Will this work?**
A: Mostly. The main difference is the Person model — 8.x uses `PS_PERSONAL_DATA` instead of `PS_PERSON` + `PS_PERS_DATA_EFFDT` + `PS_NAMES`. Auto-discovery checks for both. You may need manual overrides in `01_configure`.

**Q: Our tables have custom prefixes (e.g., `XX_PS_JOB`).**
A: Use the manual override cell in `01_configure`:
```python
found["ps_job"] = "my_catalog.my_schema.XX_PS_JOB"
```

**Q: We use Enterprise Learning Management (ELM), not legacy PS_TRAINING.**
A: Override the training table mapping to point to `PS_LM_CI_LRNHIST` or your ELM history table. The view will need column name adjustments — the ATTENDANCE and TRAINING_REASON fields may differ.

**Q: Can we add more metrics later?**
A: Yes. Metric views are just SQL — `ALTER VIEW mv_headcount_workforce AS $$ ... $$` to add dimensions or measures. You can also create additional metric views for new domains.

**Q: How do we refresh the data?**
A: You don't need to refresh anything on the Databricks side. The views query your PS tables at runtime. When your ETL updates the source tables, the views automatically reflect the new data.

**Q: What if we want to track turnover trends over time?**
A: Create a monthly snapshot table from `vw_employee_roster` (scheduled job that writes headcount by dept/month). The accelerator includes a `turnover_monthly` pattern you can adopt.

**Q: Does this work with federated catalogs (Foreign Catalog)?**
A: Yes. If your PeopleSoft data is in Oracle and accessed via a Unity Catalog foreign catalog, just point `source_catalog` and `source_schema` to the foreign catalog path. Views will query through federation.

---

## Support

Built by Databricks Field Engineering. For questions, customization help, or to request additional PeopleSoft modules (Financials, Supply Chain, CRM), contact your Databricks SA.
