# workforce-delta-warehouse

A data engineering project built on **Databricks + Delta Lake**, focused on HR and workforce analytics. The pipeline implements **SCD Type 2** on an employee dimension — preserving the full history of every department transfer, promotion, manager change, and status update.

Written entirely in **SQL**. No PySpark.

---

## The Problem

HR analytics teams ask questions that span time:

- *Who was in Engineering at the start of Q3?*
- *What was someone's job title when they got their last performance bonus?*
- *Show me the full promotion history for this employee.*
- *How has our department headcount changed quarter over quarter?*

None of these are answerable if you overwrite employee records when something changes. SCD2 solves this by keeping every version of a row — the old one gets closed with an end date, and the new state is inserted as a fresh active record.

---

## Architecture

```
┌─────────────────────────┐
│    Source: HRIS System  │
│  (Workday / SAP / BambooHR) │
└───────────┬─────────────┘
            │ daily change feed
            ▼
┌─────────────────────────┐
│    Staging View         │  ← incoming employee changes as a temp view
│  (staging_employees)    │
└───────────┬─────────────┘
            │
            ▼
┌───────────────────────────────────────────────────┐
│              SCD2 MERGE — Two Passes               │
│                                                   │
│  Pass 1: Expire changed records                   │
│    MERGE: active rows where data changed          │
│    → eff_end_date = today - 1, is_current = false │
│                                                   │
│  Pass 2: Insert new active versions               │
│    INSERT: rows with no current match             │
│    → surrogate_key via MAX + ROW_NUMBER()         │
│    → eff_start_date = today, eff_end_date = 9999  │
└───────────┬───────────────────────────────────────┘
            │
            ▼
┌───────────────────────────────────────────────────┐
│          workforce.employee_dim                   │
│          (Delta Lake table)                       │
│                                                   │
│  surrogate_key  │ employee_id  │ department  ...  │
│  eff_start_date │ eff_end_date │ is_current       │
└──────────┬────────────────────────────────────────┘
           │
     ┌─────┴───────────────┐
     ▼                     ▼
┌──────────────┐   ┌────────────────────┐
│ Headcount /  │   │ Point-in-time      │
│ Org Reports  │   │ Historical Queries │
│ (is_current) │   │ (any past date)    │
└──────────────┘   └────────────────────┘
```

---

## Tech Stack

| Layer | Tool |
|-------|------|
| Compute | Databricks (Community / AWS) |
| Storage format | Delta Lake |
| Language | **SQL only** — no PySpark |
| Version control | Git + GitHub |

---

## What's Implemented

### `notebooks/scd_type2_employee_dim.sql`

Full SCD2 pipeline for the employee dimension, written entirely in SQL.

**Covers:**
- Why employee data is a textbook SCD2 case — with a column-by-column breakdown of what needs history vs. what doesn't
- Table design with reasoning for each column, including the surrogate key and why `manager_id` stores the natural key instead of the surrogate
- Initial full load using `INSERT INTO ... SELECT` with `ROW_NUMBER()` for surrogate key generation
- Staging data with 7 scenarios: promotion, department transfer, manager change, location move, termination, new hire, and no-change pass-through
- Two-pass MERGE in SQL with inline explanation of why a single pass doesn't work
- Post-load validation: full dimension state, current workforce snapshot, point-in-time query, career progression query, headcount comparison across two time periods
- Data quality assertions: duplicate active key check, overlapping version range check
- Delta time travel for rollback

---

## Key Design Decisions

**Surrogate key without a sequence object**
Databricks Community Edition doesn't have `GENERATED ALWAYS AS IDENTITY` outside Unity Catalog. Surrogate keys are generated as `MAX(existing) + ROW_NUMBER()` in a subquery — deterministic, collision-free, pure SQL.

**`manager_id` stores the natural key, not the surrogate key**
Storing the surrogate key of a manager would break every time the manager gets promoted (new surrogate key, new row). The natural key stays stable across versions.

**Termination is a new SCD2 version, not a delete**
Hard-deleting a terminated employee's record would orphan every fact row (payroll, project assignments) that joined on their surrogate key. Termination is just another state change — it creates a new row.

**Overlap check as a post-load assertion**
If the two MERGE passes ever run out of order or a job retries, you can end up with overlapping `eff_start_date / eff_end_date` ranges. A point-in-time query on that employee then silently returns two rows. The overlap check catches this immediately.

---

## Running the Notebook

1. Open your Databricks workspace
2. Go to **Workspace → /workforce-delta-warehouse → scd_type2_employee_dim**  
   *(or import `notebooks/scd_type2_employee_dim.sql` via Workspace → Import)*
3. Attach a running cluster (DBR 11.x+ recommended)
4. Run all cells top to bottom

No external data needed — everything is self-contained.

---

## Project Structure

```
workforce-delta-warehouse/
│
└── notebooks/
    └── scd_type2_employee_dim.sql   ← SCD2 pipeline (SQL only)
```

---

## Related Projects

| Repo | Pattern | Language | Domain |
|------|---------|----------|--------|
| [retail-delta-warehouse](https://github.com/Techkrish1/retail-delta-warehouse) | SCD Type 2 | PySpark | Customer dimension |
| [retail-product-catalog](https://github.com/Techkrish1/retail-product-catalog) | SCD Type 1 | SQL | Product master data |
| **workforce-delta-warehouse** | **SCD Type 2** | **SQL** | **Employee dimension** |
