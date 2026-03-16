"""
Demo Script: Genie Queries, Data Quality, and Time Travel
Complete demonstration of key Databricks features
"""

from databricks.sdk import WorkspaceClient
from datetime import datetime

w = WorkspaceClient(profile="DEFAULT")

CATALOG = "actuary_corpfin"
WAREHOUSE_ID = "4b9b953939869799"

def exec_sql(sql):
    """Execute SQL and return results"""
    try:
        result = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=sql,
            catalog=CATALOG,
            wait_timeout="30s"
        ).result

        if result and result.data_array:
            return result.data_array
        return None
    except Exception as e:
        return f"Error: {e}"

def print_section(title):
    """Print formatted section header"""
    print("\n" + "="*80)
    print(f"  {title}")
    print("="*80 + "\n")

# ============================================================================
# PART 1: GENIE QUERY EXAMPLES
# ============================================================================

print_section("PART 1: GENIE NATURAL LANGUAGE QUERIES")

print("Question 1: Which triangles are showing deterioration above 10%?\n")
result = exec_sql("""
    SELECT
        product, state, accident_year, dev_period,
        ROUND(variance_pct, 1) as variance_pct,
        rag_status
    FROM actuary_corpfin.gold.actual_vs_expected
    WHERE variance_pct > 10
    ORDER BY variance_pct DESC
    LIMIT 10
""")

if result and not isinstance(result, str):
    print("ANSWER:")
    print(f"  Found {len(result)} triangles deteriorating >10%")
    print("\n  Top deteriorating triangles:")
    for i, row in enumerate(result[:5], 1):
        print(f"    {i}. {row[0]} in {row[1]}, AY{row[2]}, {row[3]}mo dev: {row[4]}% variance ({row[5]})")
else:
    print(f"  {result}")

print("\n" + "-"*80)

print("\nQuestion 2: What are the top 5 largest claims?\n")
result = exec_sql("""
    SELECT
        claim_id, product, state, peril,
        CAST(current_incurred AS INT) as incurred,
        status, quarters_open
    FROM actuary_corpfin.gold.large_loss_register
    ORDER BY current_incurred DESC
    LIMIT 5
""")

if result and not isinstance(result, str):
    print("ANSWER:")
    print(f"  Top 5 largest claims:\n")
    for i, row in enumerate(result, 1):
        incurred = int(row[4]) if row[4] else 0
        print(f"    {i}. Claim {row[0]}: ${incurred:,}")
        print(f"       Product: {row[1]}, State: {row[2]}, Peril: {row[3]}")
        print(f"       Status: {row[5]}, Open for {row[6]} quarters\n")
else:
    print(f"  {result}")

print("-"*80)

print("\nQuestion 3: Which cohorts have loss ratios above 100%?\n")
result = exec_sql("""
    SELECT
        cohort_id, policy_count,
        ROUND(loss_ratio, 1) as loss_ratio,
        claim_count, loss_ratio_flag
    FROM actuary_corpfin.gold.ifrs17_cohorts
    WHERE loss_ratio > 100
    ORDER BY loss_ratio DESC
    LIMIT 5
""")

if result and not isinstance(result, str):
    print("ANSWER:")
    print(f"  Found {len(result)} cohorts with loss ratio >100%")
    print("\n  Worst performing cohorts:")
    for i, row in enumerate(result[:5], 1):
        print(f"    {i}. {row[0]}: Loss Ratio {row[2]}% ({row[4]})")
        print(f"       {row[1]} policies, {row[3]} claims\n")
else:
    print(f"  {result}")

print("-"*80)

print("\nQuestion 4: Compare total incurred by product\n")
result = exec_sql("""
    SELECT
        product,
        CAST(SUM(cumulative_incurred) AS BIGINT) as total_incurred,
        CAST(SUM(cumulative_paid) AS BIGINT) as total_paid,
        SUM(claim_count) as total_claims
    FROM actuary_corpfin.gold.development_triangles
    GROUP BY product
    ORDER BY total_incurred DESC
""")

if result and not isinstance(result, str):
    print("ANSWER:")
    print("  Product Performance Summary:\n")
    for row in result:
        incurred = int(row[1]) if row[1] else 0
        paid = int(row[2]) if row[2] else 0
        claims = int(row[3]) if row[3] else 0
        paid_ratio = (paid / incurred * 100) if incurred > 0 else 0
        print(f"    {row[0].upper()}:")
        print(f"      Total Incurred: ${incurred:,}")
        print(f"      Total Paid: ${paid:,} ({paid_ratio:.1f}%)")
        print(f"      Total Claims: {claims:,}\n")
else:
    print(f"  {result}")

print("-"*80)

print("\nQuestion 5: What percentage of large losses are still open?\n")
result = exec_sql("""
    SELECT
        status,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage
    FROM actuary_corpfin.gold.large_loss_register
    GROUP BY status
    ORDER BY count DESC
""")

if result and not isinstance(result, str):
    print("ANSWER:")
    print("  Large Loss Status Breakdown:\n")
    for row in result:
        print(f"    {row[0].upper()}: {row[1]} claims ({row[2]}%)")
else:
    print(f"  {result}")

# ============================================================================
# PART 2: DATA QUALITY DEMONSTRATION
# ============================================================================

print_section("PART 2: DATA QUALITY (DQ) MONITORING")

print("DQ Expectation 1: Checking for paid > incurred violations\n")
result = exec_sql("""
    SELECT
        error_type,
        COUNT(*) as violation_count
    FROM actuary_corpfin.silver.dq_quarantine
    WHERE error_type = 'paid_exceeds_incurred'
    GROUP BY error_type
""")

if result and not isinstance(result, str):
    if len(result) > 0:
        print(f"  ⚠ Found {result[0][1]} records where paid > incurred")
        print(f"  These records are QUARANTINED for investigation")
    else:
        print(f"  ✓ No paid > incurred violations found")
else:
    print(f"  No quarantine table data yet (run DLT pipeline to populate)")

print("\n" + "-"*80)

print("\nDQ Expectation 2: Checking for future lodgement dates\n")
result = exec_sql("""
    SELECT
        error_type,
        COUNT(*) as violation_count
    FROM actuary_corpfin.silver.dq_quarantine
    WHERE error_type = 'future_lodgement'
    GROUP BY error_type
""")

if result and not isinstance(result, str):
    if len(result) > 0:
        print(f"  ⚠ Found {result[0][1]} records with future lodgement dates")
        print(f"  These records are QUARANTINED for investigation")
    else:
        print(f"  ✓ No future lodgement violations found")
else:
    print(f"  No quarantine data yet")

print("\n" + "-"*80)

print("\nDQ Summary: All Data Quality Expectations\n")
print("""
  The DLT pipeline enforces 6 DQ expectations:

  1. ✓ valid_dates: Expiry date > Inception date
  2. ✓ valid_state: State in valid list (NSW, VIC, QLD, etc.)
  3. ✓ positive_premium: Annual premium > 0
  4. ✓ incurred_gte_paid: Incurred ≥ Paid cumulative
  5. ✓ valid_lodgement: Lodgement ≥ Accident date
  6. ✓ valid_status: Status in (open, closed, reopened)

  Records failing expectations are:
  - QUARANTINED (not dropped silently)
  - Available in silver.dq_quarantine table
  - Tracked in DLT event log
  - Visible to actuaries for investigation

  DEMO TIP: This is a key differentiator vs traditional ETL
  where bad data is often dropped without visibility!
""")

# ============================================================================
# PART 3: TIME TRAVEL DEMONSTRATION
# ============================================================================

print_section("PART 3: DELTA LAKE TIME TRAVEL")

print("Time Travel Use Case: \"What did the triangle look like last quarter?\"\n")

# Get current triangle data
print("Current Data (as of today):\n")
result_current = exec_sql("""
    SELECT
        product, state, accident_year, dev_period,
        CAST(cumulative_incurred AS INT) as incurred
    FROM actuary_corpfin.gold.development_triangles
    WHERE product = 'motor' AND state = 'NSW' AND accident_year = 2024
    ORDER BY dev_period
    LIMIT 5
""")

if result_current and not isinstance(result_current, str):
    print("  Motor NSW 2024 Triangle (Current):")
    for row in result_current:
        incurred = int(row[4]) if row[4] else 0
        print(f"    {row[3]}mo development: ${incurred:,} incurred")
else:
    print(f"  {result_current}")

print("\n" + "-"*80)

# Get table history
print("\nTable Version History:\n")
result_history = exec_sql("""
    DESCRIBE HISTORY actuary_corpfin.gold.development_triangles
    LIMIT 5
""")

if result_history and not isinstance(result_history, str):
    print(f"  Found {len(result_history)} versions in history")
    print("\n  Recent versions:")
    for i, row in enumerate(result_history[:3], 1):
        # Version, timestamp, operation
        print(f"    Version {row[0]}: {row[1]} - {row[2]}")
else:
    print(f"  {result_history}")

print("\n" + "-"*80)

print("\nTime Travel Query Examples:\n")

print("""
  1. Query data as of specific timestamp:

     SELECT * FROM actuary_corpfin.gold.development_triangles
     TIMESTAMP AS OF '2025-12-31T00:00:00Z'

  2. Query data as of specific version:

     SELECT * FROM actuary_corpfin.gold.development_triangles
     VERSION AS OF 5

  3. Compare current vs prior quarter:

     WITH current AS (
         SELECT product, SUM(cumulative_incurred) as current_incurred
         FROM actuary_corpfin.gold.development_triangles
         GROUP BY product
     ),
     prior AS (
         SELECT product, SUM(cumulative_incurred) as prior_incurred
         FROM actuary_corpfin.gold.development_triangles
         TIMESTAMP AS OF '2025-09-30T00:00:00Z'
         GROUP BY product
     )
     SELECT
         c.product,
         c.current_incurred,
         p.prior_incurred,
         (c.current_incurred - p.prior_incurred) as movement
     FROM current c
     JOIN prior p ON c.product = p.product

  TIME TRAVEL BENEFITS:
  - ✓ Regulatory audit: "What did we report last quarter?"
  - ✓ Variance analysis: Compare reserve estimates over time
  - ✓ Mistake recovery: Restore accidentally deleted data
  - ✓ Reproducibility: Recreate any prior analysis
  - ✓ No need to re-run jobs: Just time travel to that date!
""")

# ============================================================================
# PART 4: DEMO TALKING POINTS
# ============================================================================

print_section("PART 4: DEMO TALKING POINTS & KEY MESSAGES")

print("""
1. GENIE NATURAL LANGUAGE QUERIES:
   • Actuaries ask questions in plain English
   • No SQL expertise required
   • Instant self-service analytics
   • Understands domain terminology (triangles, dev periods, loss ratios)

2. DATA QUALITY EMBEDDED IN PIPELINE:
   • DQ expectations are CODE (not external validation)
   • Quarantine pattern: Investigate, don't drop
   • Complete visibility in DLT event log
   • Actuaries can see what failed and why

3. TIME TRAVEL FOR AUDIT & ANALYSIS:
   • Regulatory question: "What did we know when?"
   • No need to re-run jobs to recreate prior state
   • Built-in audit trail for compliance
   • Compare estimates across valuation periods

4. UNITY CATALOG AS DATA DICTIONARY:
   • Comments, tags, lineage in one place
   • Always current (no stale Word docs)
   • Complete audit trail of who accessed what when

5. SERVERLESS DLT:
   • Zero cluster management
   • Auto-scale on demand
   • Pay only for what you use
   • Production-grade reliability

VS TRADITIONAL ACTUARIAL SYSTEMS:
   • SAS/Excel: Hours to rebuild triangles → Databricks: Seconds
   • Manual spreadsheets → Live dashboards
   • No version control → Delta time travel
   • Siloed data → Unified catalog
   • IT bottleneck → Self-service Genie
""")

print("\n" + "="*80)
print("DEMO COMPLETE!")
print("="*80 + "\n")

print("Files ready for manual UI steps:")
print("  • Genie Space: genie_instructions.md")
print("  • Dashboard: SQL files 1-4")
print("  • Pipeline: https://e2-demo-field-eng.cloud.databricks.com/#joblist/pipelines/2a52433e-beb8-446d-9091-e40854f9bd88")
