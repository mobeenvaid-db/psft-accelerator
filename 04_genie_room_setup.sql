-- Databricks notebook source
-- MAGIC %md
-- MAGIC # PeopleSoft HCM Genie Room Accelerator
-- MAGIC ## Step 5: Genie Room Configuration
-- MAGIC
-- MAGIC This notebook provides everything needed to configure the Genie Room:
-- MAGIC 1. Data objects to add
-- MAGIC 2. Instructions to paste
-- MAGIC 3. Sample questions
-- MAGIC 4. Validation queries

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Genie Room Setup
-- MAGIC
-- MAGIC ### Create the Genie Room
-- MAGIC 1. Go to **Genie** in the left sidebar
-- MAGIC 2. Click **New** > **Genie space**
-- MAGIC 3. Name: **PeopleSoft HCM Analytics**
-- MAGIC 4. Select your SQL warehouse
-- MAGIC
-- MAGIC ### Add Data Sources (Configure > Data > Add)
-- MAGIC Add these objects in this order (metric views first, then supporting views):
-- MAGIC
-- MAGIC | Priority | Object | Type | Purpose |
-- MAGIC |----------|--------|------|---------|
-- MAGIC | 1 | `mv_headcount_workforce` | Metric View | Headcount, tenure, workforce composition |
-- MAGIC | 2 | `mv_compensation` | Metric View | Salary, pay equity, labor cost |
-- MAGIC | 3 | `mv_training_compliance` | Metric View | Training completion, compliance rates |
-- MAGIC | 4 | `mv_benefits` | Metric View | Benefits enrollment and cost |
-- MAGIC | 5 | `vw_employee_roster` | View | Detailed employee-level queries |
-- MAGIC | 6 | `turnover_monthly` | Table | Pre-aggregated turnover trends |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Genie Room Instructions
-- MAGIC
-- MAGIC Copy everything below into **Configure > Instructions > Text**:
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC **About This Data**
-- MAGIC
-- MAGIC You are answering questions about Human Capital Management (HCM) data sourced from Oracle PeopleSoft HCM 9.2. The data is structured using actual PeopleSoft table names and column names with denormalized views and UC Metric Views for easy querying.
-- MAGIC
-- MAGIC **Key Business Context**
-- MAGIC - This is a healthcare workforce (home health, hospice, and corporate functions)
-- MAGIC - The workforce is approximately 70% clinical (nurses, therapists, aides) and 30% corporate/admin
-- MAGIC - Clinical staff are revenue-generating; tracking their headcount, turnover, and productivity is a top priority
-- MAGIC - CMS (Centers for Medicare & Medicaid Services) and Joint Commission compliance training is mandatory for clinical staff
-- MAGIC - Employee turnover in home health typically runs 20-30% annualized
-- MAGIC
-- MAGIC **How to Query**
-- MAGIC - Use Metric Views (mv_*) as the primary data source — they have pre-defined, governed KPI definitions
-- MAGIC - Metric view queries use MEASURE() syntax: `SELECT dimension, MEASURE(measure_name) FROM mv_name GROUP BY ALL`
-- MAGIC - Use vw_employee_roster for detailed employee-level queries that need columns not in the metric views
-- MAGIC - Always filter EMPL_STATUS = 'A' for active employees (metric views handle this automatically)
-- MAGIC
-- MAGIC **Compensation Notes**
-- MAGIC - ANNUAL_RT is the annualized rate regardless of pay frequency — use this for cross-employee comparisons
-- MAGIC - HOURLY_RT is populated for non-exempt/hourly employees
-- MAGIC - COMPRATE is the rate in the employee's COMP_FREQUENCY (could be annual, hourly, monthly)
-- MAGIC
-- MAGIC **PeopleSoft Terminology**
-- MAGIC - EMPLID = Employee ID (unique person identifier)
-- MAGIC - EMPL_RCD = Employment record number (0 = primary job; supports concurrent jobs)
-- MAGIC - EFFDT = Effective date (PeopleSoft uses row-effective dating for history)
-- MAGIC - ACTION codes: HIR=Hire, PRO=Promotion, TER=Termination, PAY=Pay Rate Change, XFR=Transfer, LOA=Leave
-- MAGIC - EMPL_STATUS: A=Active, T=Terminated, L=Leave, R=Retired, S=Suspended
-- MAGIC - FULL_PART_TIME: F=Full-Time, P=Part-Time
-- MAGIC - FLSA_STATUS: E=Exempt (salaried, no OT), N=Non-Exempt (hourly, OT eligible)
-- MAGIC - PLAN_TYPE for benefits: 1x=Medical, 2x=Dental, 3x=Vision, 4x=Savings/401k, 6x=Life, 7x=Disability
-- MAGIC - COVRG_CD: 1=Employee Only, 2=EE+Spouse, 3=EE+Children, 4=Family
-- MAGIC - TRC (Time Reporting Code): REG=Regular, OVT=Overtime, VAC=Vacation, SCK=Sick, HOL=Holiday
-- MAGIC - ATTENDANCE for training: C=Completed, E=Enrolled, W=Waitlisted, N=No-Show

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Sample Questions to Add
-- MAGIC
-- MAGIC Add these to **Configure > Settings > Sample questions**:
-- MAGIC
-- MAGIC **Headcount & Workforce**
-- MAGIC 1. How many active employees do we have?
-- MAGIC 2. What is our headcount by department?
-- MAGIC 3. Show me headcount by state
-- MAGIC 4. What percentage of our workforce is clinical?
-- MAGIC 5. How does headcount break down by tenure band?
-- MAGIC
-- MAGIC **Compensation**
-- MAGIC 6. What is the average salary by job family?
-- MAGIC 7. Show me a pay equity comparison by gender and job family
-- MAGIC 8. What is our total annual payroll cost?
-- MAGIC 9. What is the median salary for nurses?
-- MAGIC 10. Compare average pay for exempt vs non-exempt employees
-- MAGIC
-- MAGIC **Turnover**
-- MAGIC 11. What is our monthly turnover trend?
-- MAGIC 12. Which departments have the highest turnover?
-- MAGIC 13. How many open positions do we have by region?
-- MAGIC
-- MAGIC **Benefits**
-- MAGIC 14. What is our medical enrollment rate?
-- MAGIC 15. What is the average annual employee benefit cost by plan type?
-- MAGIC 16. How does coverage level (EE only vs Family) break down?
-- MAGIC
-- MAGIC **Training & Compliance**
-- MAGIC 17. What is our overall compliance training completion rate?
-- MAGIC 18. Which courses have the lowest completion rates?
-- MAGIC 19. How many clinical staff are overdue on required training?
-- MAGIC
-- MAGIC **Demographics**
-- MAGIC 20. What is the age distribution of our workforce?
-- MAGIC 21. Show me the ethnicity breakdown by department
-- MAGIC 22. What is our gender split for clinical vs non-clinical roles?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Validation Queries
-- MAGIC Run these to confirm everything is working:

-- COMMAND ----------

-- MAGIC %python
-- MAGIC catalog = dbutils.widgets.get("catalog")
-- MAGIC schema = dbutils.widgets.get("schema")
-- MAGIC fq = f"{catalog}.{schema}"

-- COMMAND ----------

-- Test metric views
SELECT service_line, MEASURE(headcount), MEASURE(avg_tenure_years), MEASURE(clinical_pct)
FROM mv_headcount_workforce
GROUP BY ALL

-- COMMAND ----------

SELECT job_family, MEASURE(avg_annual_salary), MEASURE(median_annual_salary), MEASURE(employee_count)
FROM mv_compensation
GROUP BY ALL
ORDER BY MEASURE(avg_annual_salary) DESC

-- COMMAND ----------

SELECT course_title, MEASURE(compliance_rate), MEASURE(completed), MEASURE(not_completed)
FROM mv_training_compliance
GROUP BY ALL
ORDER BY MEASURE(compliance_rate)

-- COMMAND ----------

SELECT benefit_category, MEASURE(enrolled_count), MEASURE(avg_annual_ee_cost)
FROM mv_benefits
GROUP BY ALL

-- COMMAND ----------

-- Employee roster spot check
SELECT
  COUNT(*) as total_rows,
  COUNT(DISTINCT EMPLID) as unique_employees,
  SUM(CASE WHEN EMPL_STATUS = 'A' THEN 1 ELSE 0 END) as active,
  SUM(CASE WHEN EMPL_STATUS = 'T' THEN 1 ELSE 0 END) as terminated,
  SUM(CASE WHEN CLINICAL_FLAG = 'Clinical' THEN 1 ELSE 0 END) as clinical,
  COUNT(DISTINCT STATE) as states,
  COUNT(DISTINCT DEPTID) as departments
FROM vw_employee_roster

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Next Steps
-- MAGIC
-- MAGIC ### Replacing Sample Data with Real PeopleSoft Extracts
-- MAGIC
-- MAGIC The tables are structured to accept data directly from PeopleSoft extracts:
-- MAGIC
-- MAGIC **Option A: Lakeflow Connect** (recommended)
-- MAGIC - Set up a Lakeflow Connect ingestion from your PeopleSoft/Oracle database
-- MAGIC - Map source tables directly to the target tables (column names match)
-- MAGIC
-- MAGIC **Option B: Bulk Extract + Load**
-- MAGIC - Export from PeopleSoft via Application Engine, SQR, or Data Mover
-- MAGIC - Land files in cloud storage (S3/ADLS/GCS)
-- MAGIC - Use COPY INTO or Auto Loader to load into the tables
-- MAGIC
-- MAGIC **Option C: Federation**
-- MAGIC - Create a foreign catalog pointing to your PeopleSoft Oracle database
-- MAGIC - Create views on top that reference the federated tables
-- MAGIC
-- MAGIC ### Customizing the SERVICE_LINE Dimension
-- MAGIC
-- MAGIC The `vw_employee_roster.SERVICE_LINE` field defaults to 'Unassigned'. Edit the view to map
-- MAGIC your DEPTID ranges to your actual service lines. Example:
-- MAGIC ```sql
-- MAGIC CASE
-- MAGIC   WHEN j.DEPTID BETWEEN '100' AND '199' THEN 'Home Health'
-- MAGIC   WHEN j.DEPTID BETWEEN '200' AND '299' THEN 'Hospice'
-- MAGIC   WHEN j.DEPTID BETWEEN '300' AND '399' THEN 'Pharmacy'
-- MAGIC   ELSE 'Corporate'
-- MAGIC END as SERVICE_LINE
-- MAGIC ```
-- MAGIC
-- MAGIC ### Customizing the CLINICAL_FLAG Dimension
-- MAGIC
-- MAGIC Edit `vw_employee_roster` to map your JOB_FAMILY values to clinical vs non-clinical.
-- MAGIC The default uses: Nursing, Therapy, Aide, Clinical Leadership = Clinical.
