-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Workforce Analytics — Employee Dimension (SCD Type 2)
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ## Business Context
-- MAGIC
-- MAGIC HR and workforce analytics teams run questions that are impossible to answer without change history:
-- MAGIC
-- MAGIC - *Which department was an employee in when they completed that training in 2023?*
-- MAGIC - *How many people were in Engineering at the start of each quarter?*
-- MAGIC - *What was someone's job title when they got their last performance bonus?*
-- MAGIC - *Track the career progression of employees who eventually became managers.*
-- MAGIC
-- MAGIC Employee data changes frequently — promotions, team transfers, manager reassignments,
-- MAGIC location moves. A plain `UPDATE` destroys this history. **SCD Type 2** preserves every version
-- MAGIC of an employee's record so all of those questions stay answerable, forever.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ## Why Employee Dimension is a Classic SCD2 Case
-- MAGIC
-- MAGIC | Attribute | Does history matter? | Verdict |
-- MAGIC |-----------|---------------------|---------|
-- MAGIC | Department | Yes — project attribution, headcount by period | SCD2 |
-- MAGIC | Job title | Yes — promotion history, org analytics | SCD2 |
-- MAGIC | Salary band | Yes — compensation trend analysis | SCD2 |
-- MAGIC | Manager | Yes — reporting line changes, org restructures | SCD2 |
-- MAGIC | Location | Yes — remote vs office patterns over time | SCD2 |
-- MAGIC | Employee name | Usually no — name correction is SCD1 | SCD1 exception |
-- MAGIC
-- MAGIC > In practice, most warehouses apply SCD2 to the whole row when anything changes.
-- MAGIC > The cost of storing an extra row is negligible compared to the cost of losing the ability
-- MAGIC > to answer a business question six months later.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ## Table Design
-- MAGIC
-- MAGIC | Column | Type | Purpose |
-- MAGIC |--------|------|---------|
-- MAGIC | `surrogate_key` | BIGINT | Unique row identifier — `employee_id` repeats across versions |
-- MAGIC | `employee_id` | INT | Business key from the HRIS system |
-- MAGIC | `employee_name` | STRING | Full name |
-- MAGIC | `department` | STRING | Current department for this version |
-- MAGIC | `job_title` | STRING | Title for this version |
-- MAGIC | `salary_band` | STRING | Compensation band (A–E) |
-- MAGIC | `manager_id` | INT | Reporting manager's `employee_id` |
-- MAGIC | `location` | STRING | Office / remote / city |
-- MAGIC | `employment_status` | STRING | Active, On Leave, Terminated |
-- MAGIC | `eff_start_date` | DATE | When this version became valid |
-- MAGIC | `eff_end_date` | DATE | When this version ended — `9999-12-31` = currently active |
-- MAGIC | `is_current` | BOOLEAN | Fast filter for active version |
-- MAGIC | `created_at` | TIMESTAMP | Row write timestamp |
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ## SCD2 in SQL vs PySpark
-- MAGIC
-- MAGIC Both approaches use Delta Lake's `MERGE INTO` under the hood — the logic is identical.
-- MAGIC The difference is only in how you orchestrate it.
-- MAGIC
-- MAGIC | Aspect | PySpark approach | SQL approach |
-- MAGIC |--------|-----------------|--------------|
-- MAGIC | Surrogate key generation | `row_number()` + Python arithmetic | Subquery with `ROW_NUMBER()` |
-- MAGIC | Change detection | DataFrame column comparison | MERGE condition with `<>` checks |
-- MAGIC | Readability | Verbose — schema definitions in code | Concise — DDL stays in SQL |
-- MAGIC | When to prefer | Complex transformations alongside SCD | Pure warehouse logic, no compute needed |

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Step 1: Database and Table Setup

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS workforce
  COMMENT 'Workforce analytics warehouse — HR dimensions and metrics';

-- COMMAND ----------

DROP TABLE IF EXISTS workforce.employee_dim;

-- COMMAND ----------

CREATE TABLE workforce.employee_dim (
  surrogate_key     BIGINT   COMMENT 'Unique row key — employee_id repeats across SCD2 versions',
  employee_id       INT      COMMENT 'Business key from the HRIS system',
  employee_name     STRING   COMMENT 'Full name',
  department        STRING   COMMENT 'Business unit / department',
  job_title         STRING   COMMENT 'Role title for this version',
  salary_band       STRING   COMMENT 'Compensation band: A (entry) to E (executive)',
  manager_id        INT      COMMENT 'Employee ID of direct manager; NULL for top-level',
  location          STRING   COMMENT 'Work location',
  employment_status STRING   COMMENT 'Active | On Leave | Terminated',
  eff_start_date    DATE     COMMENT 'Date this version of the record became valid',
  eff_end_date      DATE     COMMENT 'Date this version was superseded; 9999-12-31 = currently active',
  is_current        BOOLEAN  COMMENT 'True for the latest active version of this employee',
  created_at        TIMESTAMP COMMENT 'Timestamp when this row was written to the warehouse'
)
USING DELTA
COMMENT 'Employee dimension — SCD Type 2, full change history across all HR attributes'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Step 2: Initial Load
-- MAGIC
-- MAGIC First-ever full extract from the HRIS system.
-- MAGIC All employees are current — single row each, `is_current = true`, `eff_end_date = 9999-12-31`.

-- COMMAND ----------

INSERT INTO workforce.employee_dim
SELECT
  ROW_NUMBER() OVER (ORDER BY employee_id) AS surrogate_key,
  employee_id, employee_name, department, job_title,
  salary_band, manager_id, location, employment_status,
  eff_start_date, eff_end_date, is_current,
  current_timestamp() AS created_at
FROM (
  VALUES
    (101, 'Alice Fernandez', 'Engineering',  'Software Engineer',    'B', 201, 'Bangalore',  'Active',     DATE('2022-01-01'), DATE('9999-12-31'), true),
    (102, 'Ben Carter',      'Engineering',  'Software Engineer',    'B', 201, 'Bangalore',  'Active',     DATE('2022-01-01'), DATE('9999-12-31'), true),
    (103, 'Clara Singh',     'Engineering',  'Senior Engineer',      'C', 201, 'Bangalore',  'Active',     DATE('2022-01-01'), DATE('9999-12-31'), true),
    (104, 'David Kim',       'Product',      'Product Manager',      'C', 202, 'Remote',     'Active',     DATE('2022-01-01'), DATE('9999-12-31'), true),
    (105, 'Eva Patel',       'Data',         'Data Analyst',         'B', 203, 'Hyderabad',  'Active',     DATE('2022-01-01'), DATE('9999-12-31'), true),
    (106, 'Frank Ramos',     'Data',         'Data Engineer',        'C', 203, 'Hyderabad',  'Active',     DATE('2022-01-01'), DATE('9999-12-31'), true),
    (201, 'Grace Liu',       'Engineering',  'Engineering Manager',  'D', NULL,'Bangalore',  'Active',     DATE('2022-01-01'), DATE('9999-12-31'), true),
    (202, 'Henry Moss',      'Product',      'Director of Product',  'D', NULL,'Remote',     'Active',     DATE('2022-01-01'), DATE('9999-12-31'), true),
    (203, 'Irene Nair',      'Data',         'Head of Data',         'D', NULL,'Hyderabad',  'Active',     DATE('2022-01-01'), DATE('9999-12-31'), true)
) AS t(employee_id, employee_name, department, job_title, salary_band,
        manager_id, location, employment_status, eff_start_date, eff_end_date, is_current);

-- COMMAND ----------

SELECT surrogate_key, employee_id, employee_name, department, job_title, salary_band, is_current
FROM workforce.employee_dim
ORDER BY employee_id;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Step 3: Incoming Changes from HRIS
-- MAGIC
-- MAGIC This represents the **next batch of changes** from the HR system — a mix of promotions,
-- MAGIC transfers, a new hire, and a termination.
-- MAGIC
-- MAGIC | Employee | Change | What we expect |
-- MAGIC |----------|--------|----------------|
-- MAGIC | Alice (101) | Promoted: Senior Engineer, Band B → C | Expire current row, insert new version |
-- MAGIC | Ben (102) | Transferred to Data team, new manager | Expire current row, insert new version |
-- MAGIC | Clara (103) | Promoted to Staff Engineer, Band C → D | Expire current row, insert new version |
-- MAGIC | Eva (105) | Moved from Hyderabad to Remote | Expire current row, insert new version |
-- MAGIC | Frank (106) | Resigned — status → Terminated | Expire current row, insert new version |
-- MAGIC | David (104) | No changes | No action — row stays untouched |
-- MAGIC | Priya (107) | New hire joining Engineering | Insert as first active version |

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW staging_employees AS
SELECT * FROM VALUES
  (101, 'Alice Fernandez', 'Engineering', 'Senior Engineer',       'C', 201, 'Bangalore', 'Active'),
  (102, 'Ben Carter',      'Data',        'Data Engineer',         'C', 203, 'Hyderabad', 'Active'),
  (103, 'Clara Singh',     'Engineering', 'Staff Engineer',        'D', 201, 'Bangalore', 'Active'),
  (104, 'David Kim',       'Product',     'Product Manager',       'C', 202, 'Remote',    'Active'),
  (105, 'Eva Patel',       'Data',        'Data Analyst',          'B', 203, 'Remote',    'Active'),
  (106, 'Frank Ramos',     'Data',        'Data Engineer',         'C', 203, 'Hyderabad', 'Terminated'),
  (107, 'Priya Sharma',    'Engineering', 'Software Engineer',     'B', 201, 'Bangalore', 'Active')
AS t(employee_id, employee_name, department, job_title, salary_band,
      manager_id, location, employment_status);

-- COMMAND ----------

SELECT * FROM staging_employees ORDER BY employee_id;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Step 4: SCD Type 2 MERGE — Two-Pass Strategy
-- MAGIC
-- MAGIC Same two-pass pattern as any SCD2 implementation, now expressed entirely in SQL.
-- MAGIC
-- MAGIC **Why two passes:**
-- MAGIC Delta Lake's MERGE can update an existing row OR insert a new one for a given key — not both
-- MAGIC in the same statement. For SCD2, a change requires both: closing the old version and opening
-- MAGIC a new one. The only clean way to do this is sequentially.

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### Pass 1 — Expire changed records
-- MAGIC
-- MAGIC Find every active row where the incoming data differs on any tracked attribute.
-- MAGIC Close it by setting `eff_end_date = today - 1` and `is_current = false`.
-- MAGIC
-- MAGIC Using `today - 1` as the end date keeps version ranges non-overlapping:
-- MAGIC old version closes yesterday, new version opens today.

-- COMMAND ----------

MERGE INTO workforce.employee_dim AS target
USING staging_employees AS source
ON  target.employee_id = source.employee_id
AND target.is_current  = true
AND (
     target.department        <> source.department
  OR target.job_title         <> source.job_title
  OR target.salary_band       <> source.salary_band
  OR target.manager_id        <> source.manager_id
  OR target.location          <> source.location
  OR target.employment_status <> source.employment_status
)
WHEN MATCHED THEN UPDATE SET
  target.eff_end_date = DATE_SUB(CURRENT_DATE(), 1),
  target.is_current   = false;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### Pass 2 — Insert new active versions
-- MAGIC
-- MAGIC Insert a fresh row for every incoming record that no longer has an active match —
-- MAGIC which now covers both recently-expired rows and brand new employees.
-- MAGIC
-- MAGIC Surrogate keys are generated in SQL using a subquery:
-- MAGIC `MAX(existing surrogate_key) + ROW_NUMBER()` over the new rows.
-- MAGIC This guarantees uniqueness without needing a sequence object or external counter.

-- COMMAND ----------

INSERT INTO workforce.employee_dim
SELECT
  (SELECT MAX(surrogate_key) FROM workforce.employee_dim)
    + ROW_NUMBER() OVER (ORDER BY s.employee_id) AS surrogate_key,
  s.employee_id,
  s.employee_name,
  s.department,
  s.job_title,
  s.salary_band,
  s.manager_id,
  s.location,
  s.employment_status,
  CURRENT_DATE()      AS eff_start_date,
  DATE('9999-12-31')  AS eff_end_date,
  true                AS is_current,
  current_timestamp() AS created_at
FROM staging_employees s
LEFT JOIN workforce.employee_dim t
  ON  s.employee_id = t.employee_id
  AND t.is_current  = true
WHERE t.employee_id IS NULL;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Step 5: Validate the Results

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### 5a — Full dimension after SCD2 processing

-- COMMAND ----------

SELECT
  surrogate_key,
  employee_id,
  employee_name,
  department,
  job_title,
  salary_band,
  employment_status,
  eff_start_date,
  eff_end_date,
  is_current
FROM workforce.employee_dim
ORDER BY employee_id, eff_start_date;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC **What to verify:**
-- MAGIC - Alice, Ben, Clara, Eva, Frank: two rows each — one expired, one current
-- MAGIC - David (104): still one row — no change detected, untouched
-- MAGIC - Priya (107): one new row — first version, `is_current = true`
-- MAGIC - Frank (106): new row with `employment_status = Terminated`, `is_current = true`

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### 5b — Current active workforce snapshot
-- MAGIC
-- MAGIC This is what a headcount report or org chart would query.

-- COMMAND ----------

SELECT
  employee_id,
  employee_name,
  department,
  job_title,
  salary_band,
  location,
  employment_status
FROM workforce.employee_dim
WHERE is_current = true
ORDER BY department, employee_name;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### 5c — Point-in-time query
-- MAGIC
-- MAGIC *"Who was in the Engineering department on 2022-06-01?"*
-- MAGIC This returns the state of the dimension as of that date — before any of today's changes.

-- COMMAND ----------

SELECT
  employee_id,
  employee_name,
  department,
  job_title,
  salary_band,
  eff_start_date,
  eff_end_date
FROM workforce.employee_dim
WHERE department     = 'Engineering'
  AND eff_start_date <= DATE('2022-06-01')
  AND eff_end_date   >= DATE('2022-06-01')
ORDER BY employee_name;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### 5d — Promotion history for a specific employee
-- MAGIC
-- MAGIC *"Show Alice's full career progression."*
-- MAGIC This is only possible because SCD2 preserved each version.

-- COMMAND ----------

SELECT
  employee_id,
  employee_name,
  job_title,
  salary_band,
  department,
  eff_start_date,
  eff_end_date,
  is_current
FROM workforce.employee_dim
WHERE employee_id = 101
ORDER BY eff_start_date;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### 5e — Headcount by department at two points in time
-- MAGIC
-- MAGIC Demonstrates a common HR analytics use case — comparing team sizes across periods.

-- COMMAND ----------

SELECT
  department,
  COUNT(*) AS headcount_at_start_of_2022
FROM workforce.employee_dim
WHERE eff_start_date <= DATE('2022-03-01')
  AND eff_end_date   >= DATE('2022-03-01')
  AND employment_status = 'Active'
GROUP BY department
ORDER BY department;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### 5f — Data quality assertions

-- COMMAND ----------

-- No more than one active row per employee
SELECT employee_id, COUNT(*) AS active_versions
FROM workforce.employee_dim
WHERE is_current = true
GROUP BY employee_id
HAVING active_versions > 1;
-- Expect: zero rows

-- COMMAND ----------

-- Version ranges must not overlap for the same employee
SELECT a.employee_id, a.surrogate_key, b.surrogate_key
FROM workforce.employee_dim a
JOIN workforce.employee_dim b
  ON  a.employee_id    = b.employee_id
  AND a.surrogate_key  < b.surrogate_key
  AND a.eff_end_date  >= b.eff_start_date
  AND a.is_current     = false;
-- Expect: zero rows — no overlapping version ranges

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Step 6: Delta Time Travel

-- COMMAND ----------

DESCRIBE HISTORY workforce.employee_dim;

-- COMMAND ----------

-- Restore the dimension to its state right after initial load (before any SCD2 processing)
-- Uncomment to use:
-- SELECT * FROM workforce.employee_dim VERSION AS OF 1 ORDER BY employee_id;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Wrapping Up
-- MAGIC
-- MAGIC Employee data is one of the most change-heavy dimensions in any warehouse. People get promoted,
-- MAGIC move teams, change managers, go on leave, come back, get restructured — and every one of those
-- MAGIC changes is a business event that reporting teams will eventually ask questions about.
-- MAGIC
-- MAGIC A few things from this implementation that are worth internalizing:
-- MAGIC
-- MAGIC **The two-pass approach is intentional, not a limitation.**
-- MAGIC It would feel natural to try collapsing expire + insert into one MERGE statement.
-- MAGIC Delta supports `WHEN MATCHED UPDATE` and `WHEN NOT MATCHED INSERT` in the same MERGE,
-- MAGIC but you can't do both for the *same matched row* — you can't simultaneously update Alice's
-- MAGIC existing row AND insert a new row for her in a single pass. The two-pass split is the
-- MAGIC clean, production-correct solution.
-- MAGIC
-- MAGIC **The overlap check in 5f is not paranoia.**
-- MAGIC If the two passes ever run out of order, or if a retry causes a double-insert,
-- MAGIC you'll end up with overlapping date ranges for the same employee. A point-in-time query
-- MAGIC on that employee will return two rows instead of one — silently doubling headcount in reports.
-- MAGIC Running this check after every load catches that immediately.
-- MAGIC
-- MAGIC **Termination is a version, not a deletion.**
-- MAGIC Frank's termination creates a new SCD2 row with `employment_status = Terminated`.
-- MAGIC His historical rows remain intact. This is important — hard-deleting an employee's records
-- MAGIC would break any fact table (like payroll or project assignments) that joined on his surrogate key.
-- MAGIC
-- MAGIC **`manager_id` as a self-referencing key creates a subtle problem.**
-- MAGIC In this design, `manager_id` stores the employee's natural key — not their surrogate key.
-- MAGIC That's intentional: if you stored the manager's surrogate key, it would need to be updated
-- MAGIC every time the manager got a promotion (new surrogate key). Using the natural key keeps
-- MAGIC the relationship stable across versions, at the cost of needing an extra join to resolve
-- MAGIC the manager's current title.
-- MAGIC
-- MAGIC **This implementation vs. the PySpark version** (`retail-delta-warehouse`):
-- MAGIC The logic is identical. SQL is simpler to read here because the transformation is pure
-- MAGIC warehouse logic — no complex data wrangling that would benefit from DataFrame operations.
-- MAGIC In practice, if your SCD2 load also involves column derivations, null handling,
-- MAGIC or joining multiple source systems before the MERGE, PySpark becomes the more natural fit.
