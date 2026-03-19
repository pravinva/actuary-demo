# Finance Actuarial Demo - Ready for Friday

**Workspace:** https://e2-demo-field-eng.cloud.databricks.com
**Date:** 2026-03-18
**Status:** ✓ READY

---

## 1. Data Generation Complete

### Bronze Layer (Source Data)
- **policy_raw**: 10,000 rows - Australian insurance policies (Home, Motor, CTP)
- **claims_transactions_raw**: 37,437 rows - Claims transaction history

### Gold Layer (Analytics Tables)
- **development_triangles**: 399 rows - Claims development by product/state/year/period
- **actual_vs_expected**: 266 rows - Reserve deterioration monitoring with RAG status
- **large_loss_register**: 806 rows - Claims exceeding $250K threshold
- **ifrs17_cohorts**: 530 rows - IFRS 17 regulatory cohorts

All tables successfully generated in **actuary_corpfin** catalog.

---

## 2. Data Quality (DQ) - Embedded Expectations

### SDP Pipeline: actuarial_finance_serverless
**Pipeline ID:** 2a52433e-beb8-446d-9091-e40854f9bd88
**Notebook:** /Users/pravin.varma@databricks.com/actuary_demo/actuarial_dlt_serverless
**Status:** IDLE (Complete)

### Policy Data Quality Rules (3 expectations)
```python
@dlt.expect_or_drop("valid_dates", "expiry_date > inception_date")
@dlt.expect_or_drop("valid_state", "state IN ('NSW','VIC','QLD','WA','SA','TAS','ACT','NT')")
@dlt.expect_or_drop("positive_premium", "annual_premium > 0")
```

**Demo Point:** Show how invalid policies are automatically dropped before analytics

### Claims Data Quality Rules (3 expectations)
```python
@dlt.expect_or_drop("incurred_gte_paid", "incurred_cumulative >= paid_cumulative")
@dlt.expect_or_drop("valid_lodgement", "lodgement_date >= accident_date")
@dlt.expect_or_drop("valid_status", "status IN ('open','closed','reopened')")
```

**Demo Point:** Show how DQ as code enforces actuarial business rules

### How to Show DQ in Demo
1. Navigate to: https://e2-demo-field-eng.cloud.databricks.com/pipelines/2a52433e-beb8-446d-9091-e40854f9bd88
2. Click on any dataset node (e.g., "policy_bronze")
3. Show the "Data Quality" tab with expectations
4. Highlight: Zero-code DQ enforcement with automatic drop/warn/fail actions

---

## 3. Time Travel Capability

All gold tables support Delta time travel for historical analysis:

### Example Queries for Demo

**Show version history:**
```sql
DESCRIBE HISTORY actuary_corpfin.gold.development_triangles;
```

**Compare current vs original data:**
```sql
-- Current data (latest version)
SELECT COUNT(*) FROM actuary_corpfin.gold.development_triangles;

-- Original data (version 0)
SELECT COUNT(*) FROM actuary_corpfin.gold.development_triangles VERSION AS OF 0;
```

**Audit reserve changes over time:**
```sql
SELECT
  product,
  state,
  accident_year,
  SUM(cumulative_incurred) as total_incurred
FROM actuary_corpfin.gold.development_triangles VERSION AS OF 0
GROUP BY product, state, accident_year
ORDER BY total_incurred DESC
LIMIT 10;
```

**Demo Point:** Show how actuaries can audit reserve changes for regulatory compliance (IFRS 17, APRA)

---

## 4. Genie AI/BI Space

### Space Configuration
**Space Name:** Finance Actuarial Analytics
**Space ID:** 01f1225cb62218668d16941d566082e4
**URL:** https://e2-demo-field-eng.cloud.databricks.com/explore/genie/01f1225cb62218668d16941d566082e4
**Warehouse:** 4b9b953939869799

### Manual Setup Required
Due to Genie API limitations, tables must be added manually:

1. Open the Genie space URL above
2. Click "Add data source"
3. Add these 4 tables:
   - `actuary_corpfin.gold.development_triangles`
   - `actuary_corpfin.gold.actual_vs_expected`
   - `actuary_corpfin.gold.large_loss_register`
   - `actuary_corpfin.gold.ifrs17_cohorts`
4. Copy instructions from `/tmp/genie_space_config.json`

### Sample Questions to Demo

**Reserve Deterioration:**
- "Which triangles are showing deterioration above 10%?"
- "Show me all RED status reserves by product"

**Large Losses:**
- "What are the top 10 largest claims?"
- "Show me all open large losses in Victoria"

**Portfolio Analysis:**
- "Compare total incurred amounts by product"
- "Which cohorts have loss ratios above 100%?"

**Development Patterns:**
- "Show development patterns for Motor insurance in NSW"
- "Compare incurred vs paid ratios at 60 months"

**Demo Point:** Show how finance teams ask natural language questions without SQL knowledge

---

## 5. Demo Flow (Recommended)

### Act 1: The Business Problem (2 min)
- Australian insurer with complex actuarial reporting needs
- IFRS 17 compliance requirements
- Finance teams struggle with SQL and Excel-based triangles

### Act 2: Data Quality First (3 min)
- Navigate to SDP pipeline
- Show serverless compute (no cluster management)
- Highlight 6 embedded expectations
- Show data quality tab on bronze tables
- **Message:** "Trust starts with quality"

### Act 3: Natural Language Analytics (4 min)
- Open Genie space
- Ask: "Which triangles are showing deterioration above 10%?"
- Show SQL generated automatically
- Ask: "What are the top 10 largest claims?"
- Demonstrate follow-up questions
- **Message:** "Democratize analytics for finance teams"

### Act 4: Audit & Compliance (3 min)
- Open SQL editor
- Run time travel query showing version history
- Compare reserves version 0 vs current
- Show DESCRIBE HISTORY for audit trail
- **Message:** "Full lineage for regulatory compliance"

### Act 5: The Value Proposition (1 min)
- Unified platform: DQ + Analytics + Governance
- Serverless: Zero infrastructure management
- AI-powered: Natural language to insights
- Compliant: Full audit trail with time travel

---

## 6. Key Files

- **Pipeline Notebook:** actuarial_dlt_serverless.py
- **Talk Track:** talk_track.html (interactive HTML guide)
- **Genie Instructions:** genie_instructions.md
- **Genie Config:** /tmp/genie_space_config.json

---

## 7. Pre-Demo Checklist

- [ ] Verify warehouse 4b9b953939869799 is running
- [ ] Add 4 tables to Genie space manually
- [ ] Test sample questions in Genie (verify SQL generation)
- [ ] Run one time travel query to verify version history
- [ ] Navigate to SDP pipeline and verify DQ tab is visible
- [ ] Bookmark key URLs (Genie space, SDP pipeline, catalog)

---

## 8. Troubleshooting

**If Genie questions fail:**
- Check tables are added to the space
- Verify instructions are in the space settings
- Ensure warehouse is running

**If DQ expectations don't show:**
- Navigate to pipeline → Select dataset → Data Quality tab
- Expectations are embedded in code, check notebook

**If time travel fails:**
- Verify catalog permissions
- Check table versions with DESCRIBE HISTORY first

---

**Demo is ready! Good luck on Friday! 🚀**
