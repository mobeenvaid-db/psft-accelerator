# Databricks notebook source
# MAGIC %md
# MAGIC # PeopleSoft HCM Genie Room Accelerator
# MAGIC ## Step 3: Build UC Metric Views (Business Semantics)
# MAGIC
# MAGIC Creates governed metric definitions with semantic metadata (synonyms, display names, formats).
# MAGIC Genie uses the synonyms to match natural language queries to the correct dimensions and measures.
# MAGIC
# MAGIC **Requires:** Databricks Runtime 17.2+ for v1.1 semantic metadata.
# MAGIC
# MAGIC **Prerequisite:** Run `02_build_views` first.

# COMMAND ----------

dbutils.widgets.text("target_catalog", "", "Catalog for metric views")
dbutils.widgets.text("target_schema", "hcm_analytics", "Schema for metric views")

target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
tgt = f"{target_catalog}.{target_schema}"

# COMMAND ----------

# MAGIC %md
# MAGIC ### mv_headcount_workforce — 9 measures, 18 dimensions

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {tgt}.mv_headcount_workforce
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Core headcount, tenure, and workforce composition KPIs"
source: {tgt}.vw_employee_roster
filter: EMPL_STATUS IN ('A', 'T', 'L')
dimensions:
  - name: service_line
    expr: SERVICE_LINE
    display_name: 'Service Line'
    synonyms: ['business line', 'division', 'segment']
  - name: department
    expr: DEPT_NAME
    display_name: 'Department'
    synonyms: ['dept', 'team', 'org', 'unit']
  - name: department_id
    expr: DEPTID
    display_name: 'Department ID'
  - name: job_title
    expr: JOB_TITLE
    display_name: 'Job Title'
    synonyms: ['role', 'position', 'title', 'job']
  - name: job_family
    expr: JOB_FAMILY
    display_name: 'Job Family'
    synonyms: ['job group', 'role family', 'job category']
  - name: clinical_flag
    expr: CLINICAL_FLAG
    display_name: 'Clinical vs Non-Clinical'
    synonyms: ['clinical', 'patient facing']
  - name: location_name
    expr: LOCATION_NAME
    display_name: 'Branch'
    synonyms: ['branch', 'office', 'site', 'location']
  - name: city
    expr: CITY
    display_name: 'City'
  - name: state
    expr: STATE
    display_name: 'State'
    synonyms: ['state code', 'US state']
  - name: employment_type
    expr: FULL_PART_TIME_DESC
    display_name: 'Employment Type'
    synonyms: ['FT PT', 'full time part time', 'work schedule']
  - name: flsa_status
    expr: FLSA_DESC
    display_name: 'FLSA Status'
    synonyms: ['exempt status', 'overtime eligible']
  - name: employee_status
    expr: EMPL_STATUS_DESC
    display_name: 'Employee Status'
    synonyms: ['status', 'active status']
  - name: tenure_band
    expr: TENURE_BAND
    display_name: 'Tenure Band'
    synonyms: ['tenure', 'years of service band', 'seniority']
  - name: age_band
    expr: AGE_BAND
    display_name: 'Age Band'
    synonyms: ['age group', 'age range']
  - name: gender
    expr: GENDER_DESC
    display_name: 'Gender'
    synonyms: ['sex']
  - name: ethnicity
    expr: ETHNICITY_DESC
    display_name: 'Ethnicity'
    synonyms: ['race', 'ethnic group', 'EEO race', 'demographic']
  - name: hire_year
    expr: YEAR(HIRE_DT)
    display_name: 'Hire Year'
    synonyms: ['year hired', 'cohort year']
  - name: hire_month
    expr: DATE_TRUNC('MONTH', HIRE_DT)
    display_name: 'Hire Month'
    format:
      type: date
      date_format: year_month_day
measures:
  - name: headcount
    expr: COUNT(DISTINCT EMPLID) FILTER (WHERE EMPL_STATUS = 'A')
    display_name: 'Active Headcount'
    comment: "Count of active employees"
    synonyms: ['employees', 'head count', 'number of employees', 'total employees', 'how many people']
  - name: total_employees
    expr: COUNT(DISTINCT EMPLID)
    display_name: 'Total Employees All Statuses'
    synonyms: ['all employees', 'total records']
  - name: terminated_count
    expr: COUNT(DISTINCT EMPLID) FILTER (WHERE EMPL_STATUS = 'T')
    display_name: 'Terminated Employees'
    synonyms: ['terms', 'terminations', 'exits', 'separations', 'attrition count']
  - name: on_leave_count
    expr: COUNT(DISTINCT EMPLID) FILTER (WHERE EMPL_STATUS = 'L')
    display_name: 'Employees on Leave'
    synonyms: ['LOA', 'leave of absence', 'on leave']
  - name: avg_tenure_years
    expr: AVG(TENURE_YEARS) FILTER (WHERE EMPL_STATUS = 'A')
    display_name: 'Average Tenure Years'
    synonyms: ['avg tenure', 'average years of service']
    format:
      type: number
  - name: avg_age
    expr: AVG(AGE) FILTER (WHERE EMPL_STATUS = 'A')
    display_name: 'Average Age'
    synonyms: ['mean age', 'workforce age']
    format:
      type: number
  - name: clinical_headcount
    expr: COUNT(DISTINCT EMPLID) FILTER (WHERE EMPL_STATUS = 'A' AND CLINICAL_FLAG = 'Clinical')
    display_name: 'Clinical Headcount'
    synonyms: ['clinical employees', 'patient facing staff']
  - name: clinical_pct
    expr: COUNT(DISTINCT EMPLID) FILTER (WHERE EMPL_STATUS = 'A' AND CLINICAL_FLAG = 'Clinical') * 100.0 / NULLIF(COUNT(DISTINCT EMPLID) FILTER (WHERE EMPL_STATUS = 'A'), 0)
    display_name: 'Clinical Workforce Pct'
    synonyms: ['percent clinical', 'clinical ratio']
    format:
      type: percentage
  - name: full_time_pct
    expr: COUNT(DISTINCT EMPLID) FILTER (WHERE EMPL_STATUS = 'A' AND FULL_PART_TIME = 'F') * 100.0 / NULLIF(COUNT(DISTINCT EMPLID) FILTER (WHERE EMPL_STATUS = 'A'), 0)
    display_name: 'Full-Time Pct'
    synonyms: ['FT percentage', 'full time ratio']
    format:
      type: percentage
$$""")
print("mv_headcount_workforce created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### mv_compensation — 7 measures, 12 dimensions

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {tgt}.mv_compensation
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Compensation analytics and pay equity KPIs"
source: {tgt}.vw_employee_roster
filter: EMPL_STATUS = 'A' AND ANNUAL_RT IS NOT NULL
dimensions:
  - name: service_line
    expr: SERVICE_LINE
    display_name: 'Service Line'
    synonyms: ['business line', 'division']
  - name: department
    expr: DEPT_NAME
    display_name: 'Department'
    synonyms: ['dept', 'team']
  - name: job_title
    expr: JOB_TITLE
    display_name: 'Job Title'
    synonyms: ['role', 'position']
  - name: job_family
    expr: JOB_FAMILY
    display_name: 'Job Family'
    synonyms: ['job group', 'role family']
  - name: clinical_flag
    expr: CLINICAL_FLAG
    display_name: 'Clinical vs Non-Clinical'
  - name: pay_grade
    expr: GRADE
    display_name: 'Pay Grade'
    synonyms: ['grade', 'salary grade', 'comp level', 'pay band']
  - name: flsa_status
    expr: FLSA_DESC
    display_name: 'FLSA Status'
    synonyms: ['exempt', 'nonexempt']
  - name: gender
    expr: GENDER_DESC
    display_name: 'Gender'
  - name: ethnicity
    expr: ETHNICITY_DESC
    display_name: 'Ethnicity'
    synonyms: ['race', 'EEO race']
  - name: state
    expr: STATE
    display_name: 'State'
  - name: tenure_band
    expr: TENURE_BAND
    display_name: 'Tenure Band'
  - name: employment_type
    expr: FULL_PART_TIME_DESC
    display_name: 'Employment Type'
measures:
  - name: avg_annual_salary
    expr: AVG(ANNUAL_RT)
    display_name: 'Average Annual Salary'
    synonyms: ['avg salary', 'mean salary', 'average pay', 'avg comp', 'average compensation']
    format:
      type: currency
      currency_code: USD
  - name: median_annual_salary
    expr: PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ANNUAL_RT)
    display_name: 'Median Annual Salary'
    synonyms: ['median salary', 'median pay']
    format:
      type: currency
      currency_code: USD
  - name: min_salary
    expr: MIN(ANNUAL_RT)
    display_name: 'Minimum Salary'
    synonyms: ['lowest salary', 'min pay']
    format:
      type: currency
      currency_code: USD
  - name: max_salary
    expr: MAX(ANNUAL_RT)
    display_name: 'Maximum Salary'
    synonyms: ['highest salary', 'max pay', 'top salary']
    format:
      type: currency
      currency_code: USD
  - name: total_salary_cost
    expr: SUM(ANNUAL_RT)
    display_name: 'Total Annual Salary Cost'
    synonyms: ['total payroll', 'salary expense', 'labor cost', 'people cost']
    format:
      type: currency
      currency_code: USD
  - name: avg_hourly_rate
    expr: AVG(HOURLY_RT) FILTER (WHERE HOURLY_RT IS NOT NULL)
    display_name: 'Average Hourly Rate'
    synonyms: ['avg hourly', 'mean hourly rate']
    format:
      type: currency
      currency_code: USD
  - name: employee_count
    expr: COUNT(DISTINCT EMPLID)
    display_name: 'Employee Count'
    synonyms: ['headcount', 'number of employees']
$$""")
print("mv_compensation created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### mv_training_compliance — 5 measures, 6 dimensions (if training view exists)

# COMMAND ----------

try:
    spark.sql(f"""
    CREATE OR REPLACE VIEW {tgt}.mv_training_compliance
    WITH METRICS
    LANGUAGE YAML
    AS $$
version: 1.1
comment: "Training completion and compliance KPIs"
source: {tgt}.vw_training_compliance
filter: REQUIREMENT_TYPE = 'Required'
dimensions:
  - name: course_title
    expr: COURSE_TITLE
    display_name: 'Course'
    synonyms: ['training', 'class', 'course name']
  - name: completion_status
    expr: COMPLETION_STATUS
    display_name: 'Completion Status'
  - name: service_line
    expr: SERVICE_LINE
    display_name: 'Service Line'
  - name: department
    expr: DEPT_NAME
    display_name: 'Department'
    synonyms: ['dept', 'team']
  - name: clinical_flag
    expr: CLINICAL_FLAG
    display_name: 'Clinical vs Non-Clinical'
  - name: job_title
    expr: JOB_TITLE
    display_name: 'Job Title'
measures:
  - name: total_assigned
    expr: COUNT(*)
    display_name: 'Total Assigned'
  - name: completed
    expr: COUNT(*) FILTER (WHERE COMPLETION_STATUS = 'Completed')
    display_name: 'Completed'
    synonyms: ['completions', 'done']
  - name: not_completed
    expr: COUNT(*) FILTER (WHERE COMPLETION_STATUS != 'Completed')
    display_name: 'Not Yet Completed'
    synonyms: ['overdue', 'incomplete', 'outstanding']
  - name: compliance_rate
    expr: COUNT(*) FILTER (WHERE COMPLETION_STATUS = 'Completed') * 100.0 / NULLIF(COUNT(*), 0)
    display_name: 'Compliance Rate'
    synonyms: ['completion rate', 'training compliance']
    format:
      type: percentage
  - name: employees_compliant
    expr: COUNT(DISTINCT EMPLID) FILTER (WHERE COMPLETION_STATUS = 'Completed')
    display_name: 'Employees Compliant'
$$""")
    print("mv_training_compliance created.")
except Exception as e:
    print(f"SKIPPED mv_training_compliance — {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### mv_benefits — 4 measures, 5 dimensions (if benefits view exists)

# COMMAND ----------

try:
    spark.sql(f"""
    CREATE OR REPLACE VIEW {tgt}.mv_benefits
    WITH METRICS
    LANGUAGE YAML
    AS $$
version: 1.1
comment: "Benefits enrollment and cost metrics"
source: {tgt}.vw_benefits_summary
dimensions:
  - name: benefit_category
    expr: BENEFIT_CATEGORY
    display_name: 'Benefit Type'
    synonyms: ['benefit', 'plan type', 'insurance type']
  - name: benefit_plan
    expr: BENEFIT_PLAN
    display_name: 'Benefit Plan'
    synonyms: ['plan', 'plan code']
  - name: coverage_level
    expr: COVERAGE_LEVEL
    display_name: 'Coverage Level'
    synonyms: ['coverage tier', 'EE ES FAM']
  - name: service_line
    expr: SERVICE_LINE
    display_name: 'Service Line'
  - name: department
    expr: DEPT_NAME
    display_name: 'Department'
measures:
  - name: enrolled_count
    expr: COUNT(DISTINCT EMPLID)
    display_name: 'Enrolled Employees'
    synonyms: ['enrollments', 'enrolled', 'participants']
  - name: avg_ee_deduction
    expr: AVG(DEDUCTION_PER_PERIOD)
    display_name: 'Avg Employee Deduction Per Period'
    format:
      type: currency
      currency_code: USD
  - name: avg_annual_ee_cost
    expr: AVG(ANNUAL_EE_COST)
    display_name: 'Avg Annual Employee Cost'
    format:
      type: currency
      currency_code: USD
  - name: total_annual_ee_cost
    expr: SUM(ANNUAL_EE_COST)
    display_name: 'Total Annual Employee Benefit Cost'
    synonyms: ['total benefits cost', 'benefits expense']
    format:
      type: currency
      currency_code: USD
$$""")
    print("mv_benefits created.")
except Exception as e:
    print(f"SKIPPED mv_benefits — {e}")

# COMMAND ----------

print(f"\nMetric views in {tgt}:")
for row in spark.sql(f"SHOW VIEWS IN {tgt}").collect():
    if row.viewName.startswith("mv_"):
        print(f"  {row.viewName}")
