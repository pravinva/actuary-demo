# Finance Actuarial Demo — Ralph Wiggum Workstreams

**Workspace:** field engineering, default profile in `~/.databrickscfg`  
**Catalog:** `actuarial_demo`  
**Schemas:** `bronze`, `silver`, `gold`  
**Runtime:** Each workstream is independently executable in a Claude Code session. Each agent checks for and creates its own prerequisites.

---

## WS-1 — Synthetic Data Fabricator

**What it produces:** Realistic synthetic actuarial source data as Delta tables in `actuarial_demo.bronze`. All downstream workstreams depend on this data existing.

```
You are building a synthetic actuarial data generator for a Databricks demo.

Use the Databricks Python SDK with the default profile from ~/.databrickscfg.
Target catalog: actuarial_demo, schema: bronze.
Create the catalog and schema if they don't exist.

Generate and write the following Delta tables using a local Python script that
uploads data via the Databricks SDK (use dbutils or spark.write via a cluster,
or write parquet locally and upload via the Files API, whichever is cleanest
for the field engineering workspace):

TABLE 1: bronze.policy_raw
Columns: policy_id (string, UUID), product (string: home/motor/CTP, weighted 
40/40/20), state (string: NSW/VIC/QLD/WA/SA), inception_date (date, 
2018-2025), expiry_date (date, inception + 12 months), sum_insured (double), 
annual_premium (double), distribution_channel (string: direct/broker/agent), 
reinsurance_treaty_id (string), ingestion_timestamp (timestamp).
Volume: 50,000 rows. Introduce 2% DQ errors: null sum_insured, 
expiry < inception, invalid state codes.

TABLE 2: bronze.claims_transactions_raw
Columns: transaction_id (string), claim_id (string), policy_id (string, 
FK to policy_raw with 1% orphan claims), accident_date (date), 
lodgement_date (date, accident + 0-180 days), transaction_date (date), 
transaction_type (string: initial_reserve/reserve_movement/payment/recovery), 
amount (double), incurred_cumulative (double), paid_cumulative (double), 
status (string: open/closed/reopened), peril (string: 
storm/flood/fire/theft/collision/liability), large_loss_flag (boolean, 
true if incurred > 250000), development_month (int, months from accident_date 
to transaction_date).
Volume: 200,000 rows across accident years 2018-2025, development months 
0-84. Model realistic triangle shape: incurred peaks early, paid develops 
over time. CTP claims have longer tails. Introduce 2% DQ errors: 
paid > incurred on some rows, future lodgement dates.

TABLE 3: bronze.reinsurance_treaties_raw
Columns: treaty_id (string), treaty_name (string), treaty_type 
(string: QS/XL_property/XL_casualty), layer_attachment (double), 
layer_limit (double), cession_rate (double, for QS), 
reinstatement_premium_rate (double), effective_date (date), 
expiry_date (date), currency (string: AUD).
Volume: 12 treaties covering 2018-2026 in two-year tranches.

TABLE 4: bronze.finance_rates_raw
Columns: rate_date (date, monthly from 2018-01-01 to 2026-03-01), 
currency (string: AUD), 
risk_free_rate_1y/3y/5y/10y (double, realistic RBA-shaped curve), 
inflation_rate (double), fx_aud_usd (double).
Volume: ~100 rows.

After writing all tables, print row counts and a sample from each.
Print any DQ error rows introduced so they can be referenced in the demo.
```

---

## WS-2 — SDP Bronze→Silver Pipeline

**What it produces:** A deployed and running SDP pipeline in the field engineering workspace that transforms bronze→silver with embedded DQ expectations. The pipeline definition is stored as a Python file and deployed via Databricks CLI.

```
You are deploying a Spark Declarative Pipelines workflow for a Finance Actuarial demo
on Databricks.

Use the Databricks CLI (default profile in ~/.databrickscfg) and Python SDK.
Source: actuarial_demo.bronze tables (policy_raw, claims_transactions_raw, 
reinsurance_treaties_raw, finance_rates_raw).
Target: actuarial_demo.silver tables.
Create the actuarial_demo catalog and silver schema if they don't exist.

Write a single SDP Python pipeline file: actuarial_dlt_pipeline.py

The pipeline must define:

SILVER TABLE 1: silver.policy_inforce
Source: bronze.policy_raw
Transformations:
  - Derive valid_from = inception_date, valid_to = expiry_date
  - Standardise state to uppercase
  - Cast sum_insured and annual_premium to correct types
Expectations (use @dlt.expect_or_drop):
  - "valid_expiry": expiry_date > inception_date
  - "positive_sum_insured": sum_insured > 0
  - "valid_state": state IN ('NSW','VIC','QLD','WA','SA','TAS','ACT','NT')
  - "positive_premium": annual_premium > 0

SILVER TABLE 2: silver.claims_transactions
Source: bronze.claims_transactions_raw
Transformations:
  - Add ceded_amount column (join to silver.reinsurance_treaties using 
    policy_id → treaty_id, apply QS cession_rate or XL layer logic)
  - Add net_incurred = incurred_cumulative - ceded_amount
Expectations:
  - "incurred_gte_paid": incurred_cumulative >= paid_cumulative
    (use expect_or_drop)
  - "valid_lodgement": lodgement_date >= accident_date
    (use expect_or_drop)
  - "valid_status": status IN ('open','closed','reopened')
    (use expect_or_drop)
  - "no_future_lodgement": lodgement_date <= current_date()
    (use expect_or_quarantine — write failures to silver.claims_dq_quarantine)

SILVER TABLE 3: silver.reinsurance_treaties
Source: bronze.reinsurance_treaties_raw
Expectations:
  - "valid_dates": expiry_date > effective_date
  - "valid_cession": cession_rate BETWEEN 0 AND 1 OR cession_rate IS NULL

SILVER TABLE 4: silver.discount_rates
Source: bronze.finance_rates_raw
Transformations: unpivot term columns to long format 
(rate_date, term, rate_value)

After writing the pipeline file, deploy it using the Databricks SDK 
(pipelines.create or pipelines.update if exists) with:
  - catalog: actuarial_demo
  - target schema: silver  
  - development mode: true (for demo)
  - cluster: use serverless if available, else smallest available node type

Trigger a pipeline update and wait for it to complete. Print the pipeline URL.
Print the SDP event log summary: how many rows passed, dropped, quarantined 
per expectation. This is the demo's DQ audit trail.
```

---

## WS-3 — Gold Analytics Engine

**What it produces:** Gold layer tables: development triangles, actual vs expected, large loss register. These are the demo's centrepiece dashboard data sources.

```
You are building the gold analytics layer for a Finance Actuarial demo on 
Databricks.

Use the Databricks Python SDK (default profile ~/.databrickscfg).
Source: actuarial_demo.silver tables.
Target: actuarial_demo.gold tables.
Create the gold schema if it doesn't exist.

Build the following as Databricks SQL jobs (use the Jobs API or 
sql_statements API to run them, store the SQL in .sql files locally):

GOLD TABLE 1: gold.development_triangles
Logic:
  For each combination of (product, accident_year, development_month_bucket):
  - Bucket development months into standard actuarial periods: 
    3, 6, 12, 24, 36, 48, 60, 72, 84
  - Compute incremental_paid, cumulative_paid, incremental_incurred, 
    cumulative_incurred, claim_count, open_claim_count
  - Include both gross and net (after reinsurance) columns
SQL pattern:
  SELECT product, state,
    YEAR(accident_date) AS accident_year,
    FLOOR(development_month / 12) * 12 AS dev_period,
    SUM(CASE WHEN transaction_type='payment' THEN amount ELSE 0 END) 
      AS incremental_paid,
    MAX(paid_cumulative) AS cumulative_paid,
    MAX(incurred_cumulative) AS cumulative_incurred,
    MAX(net_incurred) AS net_incurred,
    COUNT(DISTINCT claim_id) AS claim_count
  FROM actuarial_demo.silver.claims_transactions
  GROUP BY 1,2,3,4
Partition by product, state.

GOLD TABLE 2: gold.actual_vs_expected
Logic:
  - Compute age-to-age development factors per triangle cell 
    (factor = cumulative at period N+1 / cumulative at period N)
  - Compute the weighted average factor across all available accident years 
    for each development period (this is the "expected" / selected factor)
  - For the most recent diagonal (current quarter), compare the actual 
    emerging factor to the expected
  - Add columns: actual_factor, expected_factor, variance, 
    variance_pct, deteriorating_flag (true if actual > expected * 1.05)
This is the reserve deterioration early warning signal.

GOLD TABLE 3: gold.large_loss_register
Logic:
  One row per large claim (incurred_cumulative > 250,000 at any point).
  Columns: claim_id, policy_id, product, state, peril, accident_date,
  lodgement_date, current_incurred, current_paid, net_incurred, status,
  development_history (collect_list of (transaction_date, incurred_cumulative) 
  ordered by transaction_date as a JSON array),
  quarters_open, reinsurance_recovery_expected.

GOLD TABLE 4: gold.ifrs17_cohorts  
Logic:
  Group policies into IFRS 17 cohorts:
  Columns: cohort_id (product + state + inception_year + quarter),
  policy_count, gross_written_premium, earned_premium_ytd,
  incurred_claims_ytd, loss_ratio, claim_count,
  cohort_inception_year, cohort_inception_quarter.

After creating all tables, run a validation query for each:
  - Row counts
  - Triangle completeness check (every accident year has entries at 
    expected development periods)
  - Flag any null values in key columns
Print results. These are the demo validation outputs.
```

---

## WS-4 — Unity Catalog Metadata and Data Dictionary

**What it produces:** Column-level comments, tags, and a lineage-readable schema on all actuarial tables. This is the "Unity Catalog IS the data dictionary" demo segment.

```
You are enriching the Unity Catalog metadata for a Finance Actuarial demo 
on Databricks.

Use the Databricks Python SDK (default profile ~/.databrickscfg).
Target: all tables in actuarial_demo.bronze, actuarial_demo.silver, 
actuarial_demo.gold.

Perform the following via SDK or SQL statements:

1. COLUMN COMMENTS
Apply meaningful business comments to every column using ALTER TABLE ... 
ALTER COLUMN ... COMMENT '...'.
Key ones:
  claims_transactions.incurred_cumulative: 
    "Total incurred amount including case reserve as at transaction date. 
     Must always be >= paid_cumulative."
  claims_transactions.net_incurred: 
    "Gross incurred less reinsurance recoveries. Primary input to net IBNR."
  policy_inforce.valid_from / valid_to: 
    "Temporal validity window. Use TIMESTAMP AS OF to reconstruct in-force 
     portfolio at any valuation date."
  discount_rates.rate_value: 
    "RBA risk-free rate used for IFRS 17 discounting. Versioned — 
     use Delta time travel to retrieve rate applicable at any valuation date."
  development_triangles.deteriorating_flag (in gold.actual_vs_expected):
    "True when actual age-to-age factor exceeds expected by >5%. 
     Primary input to reserve adequacy monitoring."

2. TABLE TAGS
Apply tags using ALTER TABLE ... SET TAGS:
  All bronze tables: ("layer", "bronze"), ("domain", "actuarial")
  All silver tables: ("layer", "silver"), ("domain", "actuarial")
  gold.development_triangles: ("ifrs17_input", "true"), ("reserve_input", "true")
  gold.ifrs17_cohorts: ("ifrs17_input", "true"), ("regulatory", "apra")
  Any column containing name/address: ("pii", "true")
  gold.large_loss_register: ("board_reporting", "true")

3. TABLE COMMENTS
Apply table-level comments explaining the business purpose:
  silver.claims_transactions: 
    "Transaction-level claims history. Preserves every reserve movement, 
     payment, and recovery. Primary source for triangle construction and 
     IBNR calculation."
  gold.actual_vs_expected: 
    "Reserve deterioration monitoring. Compares emerging development factors 
     to prior selected assumptions. Updated automatically with each 
     claims data refresh."

4. GENERATE A DATA DICTIONARY
Write a Python script that queries information_schema.columns and 
information_schema.tables for the actuarial_demo catalog and produces 
a formatted markdown file: actuarial_data_dictionary.md
Include: table name, business description, column name, data type, 
business comment, tags.
Save the file locally. This is a demo artefact — "your data dictionary 
is always current because it is generated from the platform."

Print a summary of tags applied and tables documented.
```

---

## WS-5 — SQL Dashboard Layer

**What it produces:** Four SQL query files plus a Lakeflow/Databricks SQL dashboard definition covering the four Finance Actuarial panels. Queries are demo-ready with parameterisable filters.

```
You are building the SQL dashboard layer for a Finance Actuarial demo 
on Databricks.

Use the Databricks SDK (default profile ~/.databrickscfg).
Source tables: actuarial_demo.gold.*

Write the following SQL files and execute them via the SQL Statement 
Execution API to validate they return results. Save each as a .sql file.

QUERY 1: triangle_viewer.sql
Purpose: Development triangle panel — select product, state, basis.
Pivot development periods as columns.
Parameters: :product (default 'motor'), :state (default 'NSW'), 
:basis (default 'incurred')
Logic: Pivot gold.development_triangles on dev_period (12,24,36,48,60,72,84)
with accident_year as rows. Show both gross and net columns.
Include claim_count per cell.

QUERY 2: actual_vs_expected.sql
Purpose: Reserve deterioration panel. 
Show for each (product, dev_period) the expected_factor, actual_factor, 
variance_pct, and a RAG status:
  'GREEN' if variance_pct < 5%
  'AMBER' if 5% <= variance_pct < 15%  
  'RED' if variance_pct >= 15% or deteriorating_flag = true
Order by variance_pct DESC so worst cells appear first.
Filter to current accident years (2022 onwards) by default.

QUERY 3: large_loss_register.sql
Purpose: Large loss register panel.
Columns: claim_id, product, state, peril, accident_date, current_incurred, 
current_paid, net_incurred, status, quarters_open, 
reinsurance_recovery_expected, development_trend (latest 3 entries from 
development_history JSON array).
Order by current_incurred DESC.
Parameters: :min_incurred (default 250000), :status_filter (default 'open')

QUERY 4: ifrs17_cohort_summary.sql
Purpose: IFRS 17 cohort monitoring panel.
Show cohort_id, policy_count, gross_written_premium, earned_premium_ytd, 
incurred_claims_ytd, loss_ratio formatted as percentage,
cohort_inception_year, cohort_inception_quarter.
Add a loss_ratio_flag: 'ABOVE_100' / 'WATCH' (80-100%) / 'NORMAL' (<80%).
Order by loss_ratio DESC.

After writing and validating queries, create a Databricks SQL dashboard 
using the AI/BI dashboard API endpoint for dashboard creation with:
  - Four panels, one per query
  - Dashboard name: "Finance Actuarial — Portfolio Monitoring"
  - Use a SQL warehouse (find the first available warehouse via 
    warehouses.list())

Print the dashboard URL. This is the demo link.
```

---

## WS-6 — MLflow Anomaly Detection

**What it produces:** A trained isolation forest model on claims data, logged to MLflow with full provenance, plus a scoring job that writes flags to `gold.anomaly_flags`. This is the AI/ML demo segment.

```
You are building the ML anomaly detection component for a Finance Actuarial 
demo on Databricks.

Use the Databricks Python SDK and MLflow (default profile ~/.databrickscfg).
Source: actuarial_demo.silver.claims_transactions
Target: actuarial_demo.gold.anomaly_flags

The demo has two parts: triangle anomaly detection (statistical) and 
claim-level anomaly detection (ML).

PART 1: Triangle-level deterioration flags (statistical, no ML)
Read gold.actual_vs_expected.
For each row where deteriorating_flag = true, write to 
gold.anomaly_flags with:
  flag_type = 'TRIANGLE_DETERIORATION'
  entity_id = concat(product, '_', state, '_', accident_year, '_', dev_period)
  flag_reason = 'Actual development factor {actual_factor} exceeds 
                 expected {expected_factor} by {variance_pct}%'
  severity = CASE WHEN variance_pct > 20 THEN 'HIGH' 
                  WHEN variance_pct > 10 THEN 'MEDIUM' 
                  ELSE 'LOW' END
  flag_timestamp = current_timestamp()
  model_version = 'statistical_v1'

PART 2: Claim-level anomaly detection (IsolationForest)
Features: 
  - paid_to_incurred_ratio (paid_cumulative / incurred_cumulative at 12 months)
  - development_speed (incurred at 12m / incurred at 36m — a measure of 
    how fast claims develop)
  - claim_size_log (log(incurred_cumulative))
  - is_reopened (1 if status ever = 'reopened')
  - dev_month_at_first_large_movement (first month where reserve moved > 50k)
Compute these features with a PySpark aggregation over claims_transactions.

Training:
  - Filter to closed claims with full 36-month development history
  - Train sklearn IsolationForest with contamination=0.05 (flag 5% as outliers)
  - Log to MLflow experiment "actuarial_anomaly_detection":
      Log params: contamination, n_estimators, features list
      Log metrics: anomaly_rate, mean_anomaly_score
      Log the training dataset version (Delta table version number) as a tag
      Log the model as a sklearn flavour

Scoring:
  - Score all open claims using the trained model
  - Write anomalies to gold.anomaly_flags with:
      flag_type = 'LATENT_CLAIM'
      entity_id = claim_id
      flag_reason = 'Claim characteristics outside normal cluster. 
                     Anomaly score: {score}'
      severity based on anomaly score percentile

After completing both parts, print:
  - MLflow experiment URL
  - Count of flags by type and severity
  - Top 10 flagged claims with their anomaly scores
  - The MLflow run ID (to show in demo as the audit trail)
```

---

## Demo Run Order

| Order | Workstream | Demo Purpose | Time |
|---|---|---|---|
| 1 | WS-1 | "Here's our synthetic source data landing in bronze" | Setup only |
| 2 | WS-2 | Live: show SDP pipeline + event log with DQ failures | 5 min |
| 3 | WS-4 | Live: show Unity Catalog lineage + auto-generated data dictionary | 3 min |
| 4 | WS-3 | Live: query development triangle from gold | 4 min |
| 5 | WS-5 | Live: open dashboard — 4 panels, highlight A vs E RED rows | 5 min |
| 6 | WS-6 | Live: show MLflow run, then anomaly flags feeding dashboard | 4 min |

**Time travel moment (in WS-3 segment):**  
```sql
-- Show triangle as at prior valuation date
SELECT * FROM actuarial_demo.gold.development_triangles 
TIMESTAMP AS OF '2025-12-31'
-- Then show today's — triangle has moved. No SAS rerun.
```

**Genie moment (end of WS-5 segment):**  
Set up a Genie space over gold.* and ask live:  
*"Which triangles are showing deterioration above 10% for accident year 2023?"*
