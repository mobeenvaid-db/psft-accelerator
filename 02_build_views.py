# Databricks notebook source
# MAGIC %md
# MAGIC # PeopleSoft HCM Genie Room Accelerator
# MAGIC ## Step 2: Build Genie-Ready Views
# MAGIC
# MAGIC Creates denormalized views on top of **your existing PeopleSoft tables**.
# MAGIC No data is copied — these are views that join your raw PS tables at query time.
# MAGIC
# MAGIC **Prerequisite:** Run `01_configure` first to map your table names.

# COMMAND ----------

dbutils.widgets.text("source_catalog", "", "Catalog where your PS tables live")
dbutils.widgets.text("source_schema", "", "Schema where your PS tables live")
dbutils.widgets.text("target_catalog", "", "Catalog for views/metric views")
dbutils.widgets.text("target_schema", "hcm_analytics", "Schema for views/metric views")

source_catalog = dbutils.widgets.get("source_catalog")
source_schema = dbutils.widgets.get("source_schema")
target_catalog = dbutils.widgets.get("target_catalog") or source_catalog
target_schema = dbutils.widgets.get("target_schema")

src = f"{source_catalog}.{source_schema}"
tgt = f"{target_catalog}.{target_schema}"

# COMMAND ----------

# Load the table mapping from step 1
config_df = spark.table(f"{tgt}._accelerator_config")
table_map = {row.standard_name: row.actual_table for row in config_df.collect()}

def t(name):
    """Get the actual table reference for a standard PS table name."""
    if name not in table_map:
        raise ValueError(f"Table '{name}' not found in config. Run 01_configure first or add a manual override.")
    return table_map[name]

print(f"Loaded {len(table_map)} table mappings from {tgt}._accelerator_config")
for k, v in sorted(table_map.items()):
    print(f"  {k:25s} -> {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### vw_employee_roster
# MAGIC One row per employee. Joins PS_JOB + PS_PERSON + PS_NAMES + PS_PERS_DATA_EFFDT +
# MAGIC PS_EMPLOYMENT + PS_DIVERS_ETHNIC + PS_DEPT_TBL + PS_LOCATION_TBL + PS_JOBCODE_TBL + PS_EMAIL_ADDRESSES.
# MAGIC
# MAGIC **Customize:** Edit the `SERVICE_LINE` and `CLINICAL_FLAG` CASE statements below to match your organization.

# COMMAND ----------

# Build the view SQL dynamically based on which tables exist
has_ethnic = "ps_divers_ethnic" in table_map
has_email = "ps_email_addresses" in table_map

ethnic_join = f"LEFT JOIN {t('ps_divers_ethnic')} de ON j.EMPLID = de.EMPLID AND de.PRIMARY_INDICATOR = 'Y'" if has_ethnic else ""
ethnic_cols = """
  de.ETHNIC_GRP_CD,
  CASE de.ETHNIC_GRP_CD
    WHEN '1' THEN 'White' WHEN '2' THEN 'Black/African American' WHEN '3' THEN 'Hispanic/Latino'
    WHEN '4' THEN 'Asian' WHEN '5' THEN 'American Indian/Alaska Native'
    WHEN '6' THEN 'Native Hawaiian/Pacific Islander' WHEN '7' THEN 'Two or More Races'
    ELSE 'Not Specified'
  END as ETHNICITY_DESC,""" if has_ethnic else """
  NULL as ETHNIC_GRP_CD,
  'Not Available' as ETHNICITY_DESC,"""

email_join = f"LEFT JOIN {t('ps_email_addresses')} ea ON j.EMPLID = ea.EMPLID AND ea.E_ADDR_TYPE = 'BUSN'" if has_email else ""
email_col = "ea.EMAIL_ADDR," if has_email else "NULL as EMAIL_ADDR,"

view_sql = f"""
CREATE OR REPLACE VIEW {tgt}.vw_employee_roster
COMMENT 'Denormalized current-state employee roster. Joins PeopleSoft tables into one row per employee. Use EMPL_STATUS=A for active. Includes derived: TENURE_YEARS, TENURE_BAND, AGE, AGE_BAND, SERVICE_LINE, CLINICAL_FLAG.'
AS
SELECT
  j.EMPLID,
  n.FIRST_NAME,
  n.LAST_NAME,
  n.NAME as FULL_NAME,
  pd.SEX as GENDER,
  CASE pd.SEX WHEN 'M' THEN 'Male' WHEN 'F' THEN 'Female' WHEN 'U' THEN 'Unknown' WHEN 'O' THEN 'Other' ELSE pd.SEX END as GENDER_DESC,
  pd.MAR_STATUS,
  CASE pd.MAR_STATUS WHEN 'S' THEN 'Single' WHEN 'M' THEN 'Married' WHEN 'D' THEN 'Divorced' WHEN 'W' THEN 'Widowed' ELSE pd.MAR_STATUS END as MAR_STATUS_DESC,
  p.BIRTHDATE,
  FLOOR(DATEDIFF(CURRENT_DATE(), p.BIRTHDATE) / 365.25) as AGE,
  CASE
    WHEN FLOOR(DATEDIFF(CURRENT_DATE(), p.BIRTHDATE) / 365.25) < 25 THEN 'Under 25'
    WHEN FLOOR(DATEDIFF(CURRENT_DATE(), p.BIRTHDATE) / 365.25) < 35 THEN '25-34'
    WHEN FLOOR(DATEDIFF(CURRENT_DATE(), p.BIRTHDATE) / 365.25) < 45 THEN '35-44'
    WHEN FLOOR(DATEDIFF(CURRENT_DATE(), p.BIRTHDATE) / 365.25) < 55 THEN '45-54'
    ELSE '55+'
  END as AGE_BAND,
  {ethnic_cols}
  emp.HIRE_DT,
  emp.REHIRE_DT,
  emp.TERMINATION_DT,
  emp.CMPNY_SENIORITY_DT,
  ROUND(DATEDIFF(COALESCE(emp.TERMINATION_DT, CURRENT_DATE()), emp.HIRE_DT) / 365.25, 1) as TENURE_YEARS,
  CASE
    WHEN DATEDIFF(COALESCE(emp.TERMINATION_DT, CURRENT_DATE()), emp.HIRE_DT) < 365 THEN 'Under 1 Year'
    WHEN DATEDIFF(COALESCE(emp.TERMINATION_DT, CURRENT_DATE()), emp.HIRE_DT) < 730 THEN '1-2 Years'
    WHEN DATEDIFF(COALESCE(emp.TERMINATION_DT, CURRENT_DATE()), emp.HIRE_DT) < 1825 THEN '2-5 Years'
    WHEN DATEDIFF(COALESCE(emp.TERMINATION_DT, CURRENT_DATE()), emp.HIRE_DT) < 3650 THEN '5-10 Years'
    ELSE '10+ Years'
  END as TENURE_BAND,
  j.EMPL_STATUS,
  CASE j.EMPL_STATUS WHEN 'A' THEN 'Active' WHEN 'T' THEN 'Terminated' WHEN 'L' THEN 'Leave of Absence'
    WHEN 'R' THEN 'Retired' WHEN 'S' THEN 'Suspended' WHEN 'P' THEN 'Leave with Pay'
    WHEN 'W' THEN 'Short Work Break' WHEN 'D' THEN 'Deceased' ELSE j.EMPL_STATUS END as EMPL_STATUS_DESC,
  j.FULL_PART_TIME,
  CASE j.FULL_PART_TIME WHEN 'F' THEN 'Full-Time' WHEN 'P' THEN 'Part-Time' ELSE 'Per Diem' END as FULL_PART_TIME_DESC,
  j.REG_TEMP,
  j.EMPL_TYPE,
  CASE j.EMPL_TYPE WHEN 'S' THEN 'Salaried' WHEN 'H' THEN 'Hourly' ELSE j.EMPL_TYPE END as EMPL_TYPE_DESC,
  j.DEPTID,
  d.DESCR as DEPT_NAME,
  j.JOBCODE,
  jc.DESCR as JOB_TITLE,
  jc.JOB_FAMILY,
  j.LOCATION,
  l.DESCR as LOCATION_NAME,
  l.CITY,
  l.STATE,
  l.COUNTRY,
  j.FLSA_STATUS,
  CASE j.FLSA_STATUS WHEN 'E' THEN 'Exempt' WHEN 'N' THEN 'Non-Exempt' ELSE j.FLSA_STATUS END as FLSA_DESC,
  j.COMP_FREQUENCY,
  j.COMPRATE,
  j.ANNUAL_RT,
  j.HOURLY_RT,
  j.STD_HOURS,
  j.GRADE,
  j.SAL_ADMIN_PLAN,
  j.SUPERVISOR_ID,
  j.COMPANY,
  j.BUSINESS_UNIT,
  j.POSITION_NBR,
  j.ACTION as LAST_ACTION,
  j.ACTION_REASON as LAST_ACTION_REASON,
  {email_col}

  -- =====================================================================
  -- CUSTOMIZE THESE TWO FIELDS FOR YOUR ORGANIZATION
  -- =====================================================================

  -- CLINICAL_FLAG: Map your JOB_FAMILY values to clinical vs non-clinical
  -- Edit the IN list to match your organization's clinical job families
  CASE
    WHEN jc.JOB_FAMILY IN ('NRS','NURS','NURSING','THR','THER','THERAPY','AID','AIDE','CLIN','CLINICAL')
    THEN 'Clinical'
    ELSE 'Non-Clinical'
  END as CLINICAL_FLAG,

  -- SERVICE_LINE: Map your DEPTID ranges or patterns to business segments
  -- Replace this with your actual department-to-service-line mapping
  'Unassigned' as SERVICE_LINE
  -- Example:
  -- CASE
  --   WHEN j.DEPTID BETWEEN '100' AND '199' THEN 'Home Health'
  --   WHEN j.DEPTID BETWEEN '200' AND '299' THEN 'Hospice'
  --   WHEN j.DEPTID BETWEEN '300' AND '399' THEN 'Pharmacy'
  --   ELSE 'Corporate'
  -- END as SERVICE_LINE

FROM {t('ps_job')} j
JOIN {t('ps_person')} p ON j.EMPLID = p.EMPLID
JOIN {t('ps_names')} n ON j.EMPLID = n.EMPLID AND n.NAME_TYPE = 'PRI'
JOIN {t('ps_pers_data_effdt')} pd ON j.EMPLID = pd.EMPLID
JOIN {t('ps_employment')} emp ON j.EMPLID = emp.EMPLID AND j.EMPL_RCD = emp.EMPL_RCD
{ethnic_join}
LEFT JOIN {t('ps_dept_tbl')} d ON j.DEPTID = d.DEPTID AND d.EFF_STATUS = 'A'
LEFT JOIN {t('ps_location_tbl')} l ON j.LOCATION = l.LOCATION AND l.EFF_STATUS != 'I'
LEFT JOIN {t('ps_jobcode_tbl')} jc ON j.JOBCODE = jc.JOBCODE AND jc.EFF_STATUS = 'A'
{email_join}
WHERE j.EFFSEQ = 0
"""

spark.sql(view_sql)
print("vw_employee_roster created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### vw_benefits_summary (if PS_HEALTH_BENEFIT exists)

# COMMAND ----------

if "ps_health_benefit" in table_map:
    spark.sql(f"""
    CREATE OR REPLACE VIEW {tgt}.vw_benefits_summary
    COMMENT 'Benefits enrollment summary with readable category and coverage labels. Filter ENROLLMENT_STATUS=Enrolled for current.'
    AS
    SELECT
      h.EMPLID,
      r.FIRST_NAME, r.LAST_NAME, r.DEPT_NAME, r.JOB_TITLE, r.SERVICE_LINE,
      h.PLAN_TYPE,
      CASE
        WHEN h.PLAN_TYPE LIKE '1%' THEN 'Medical' WHEN h.PLAN_TYPE LIKE '2%' THEN 'Dental'
        WHEN h.PLAN_TYPE LIKE '3%' THEN 'Vision' WHEN h.PLAN_TYPE LIKE '6%' THEN 'Life Insurance'
        WHEN h.PLAN_TYPE LIKE '7%' THEN 'Disability' ELSE 'Other'
      END as BENEFIT_CATEGORY,
      h.BENEFIT_PLAN,
      h.COVERAGE_ELECT,
      CASE h.COVERAGE_ELECT WHEN 'E' THEN 'Enrolled' WHEN 'T' THEN 'Terminated' WHEN 'W' THEN 'Waived' ELSE h.COVERAGE_ELECT END as ENROLLMENT_STATUS,
      h.COVRG_CD,
      CASE h.COVRG_CD WHEN '1' THEN 'Employee Only' WHEN '2' THEN 'EE + Spouse' WHEN '3' THEN 'EE + Children' WHEN '4' THEN 'Family' ELSE h.COVRG_CD END as COVERAGE_LEVEL,
      h.DED_CUR as DEDUCTION_PER_PERIOD,
      h.DED_CUR * 26 as ANNUAL_EE_COST,
      h.COVERAGE_BEGIN_DT
    FROM {t('ps_health_benefit')} h
    JOIN {tgt}.vw_employee_roster r ON h.EMPLID = r.EMPLID
    WHERE h.COVERAGE_ELECT = 'E'
    """)
    print("vw_benefits_summary created.")
else:
    print("SKIPPED vw_benefits_summary — PS_HEALTH_BENEFIT not found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### vw_leave_balances (if PS_LEAVE_ACCRUAL exists)

# COMMAND ----------

if "ps_leave_accrual" in table_map:
    spark.sql(f"""
    CREATE OR REPLACE VIEW {tgt}.vw_leave_balances
    COMMENT 'PTO and leave balances with calculated AVAILABLE_BALANCE. Active employees only.'
    AS
    SELECT
      la.EMPLID,
      r.FIRST_NAME, r.LAST_NAME, r.DEPT_NAME, r.JOB_TITLE, r.SERVICE_LINE, r.TENURE_YEARS,
      la.BENEFIT_PLAN as LEAVE_TYPE,
      la.ACCRUAL_PROC_DT as AS_OF_DATE,
      la.HRS_CARRIED_OVER, la.HRS_EARNED_YTD, la.HRS_TAKEN_YTD, la.HRS_ADJUSTED_YTD,
      (COALESCE(la.HRS_CARRIED_OVER,0) + COALESCE(la.HRS_EARNED_YTD,0) - COALESCE(la.HRS_TAKEN_YTD,0) + COALESCE(la.HRS_ADJUSTED_YTD,0)) as AVAILABLE_BALANCE
    FROM {t('ps_leave_accrual')} la
    JOIN {tgt}.vw_employee_roster r ON la.EMPLID = r.EMPLID
    WHERE r.EMPL_STATUS = 'A'
    """)
    print("vw_leave_balances created.")
else:
    print("SKIPPED vw_leave_balances — PS_LEAVE_ACCRUAL not found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### vw_training_compliance (if PS_TRAINING exists)

# COMMAND ----------

if "ps_training" in table_map:
    spark.sql(f"""
    CREATE OR REPLACE VIEW {tgt}.vw_training_compliance
    COMMENT 'Training status with readable labels. Filter COMPLETION_STATUS=Completed and REQUIREMENT_TYPE=Required for compliance.'
    AS
    SELECT
      t.EMPLID,
      r.FIRST_NAME, r.LAST_NAME, r.DEPT_NAME, r.JOB_TITLE, r.SERVICE_LINE, r.CLINICAL_FLAG,
      t.COURSE, t.COURSE_TITLE, t.ATTENDANCE,
      CASE t.ATTENDANCE WHEN 'C' THEN 'Completed' WHEN 'E' THEN 'Enrolled' WHEN 'W' THEN 'Waitlisted' WHEN 'N' THEN 'No Show' WHEN 'X' THEN 'Cancelled' ELSE t.ATTENDANCE END as COMPLETION_STATUS,
      t.TRAINING_REASON,
      CASE t.TRAINING_REASON WHEN 'REQ' THEN 'Required' WHEN 'OPT' THEN 'Optional' WHEN 'CER' THEN 'Certification' ELSE t.TRAINING_REASON END as REQUIREMENT_TYPE,
      t.COURSE_START_DT, t.COURSE_END_DT, t.COURSE_GRADE, t.TRAINING_TYPE
    FROM {t('ps_training')} tr
    JOIN {tgt}.vw_employee_roster r ON t.EMPLID = r.EMPLID
    """)
    print("vw_training_compliance created.")
else:
    print("SKIPPED vw_training_compliance — PS_TRAINING not found.")

# COMMAND ----------

# Summarize what was created
print(f"\n{'='*60}")
print(f"Views created in {tgt}:")
views = [row.viewName for row in spark.sql(f"SHOW VIEWS IN {tgt}").collect()]
for v in sorted(views):
    if not v.startswith("_"):
        count = spark.sql(f"SELECT COUNT(*) FROM {tgt}.{v}").collect()[0][0]
        print(f"  {v:35s} {count:>10,} rows")
