"""
Verify Finance Actuarial Demo Readiness
"""

from databricks.sdk import WorkspaceClient

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

print("="*80)
print("FINANCE ACTUARIAL DEMO - READINESS CHECK")
print("="*80)

print(f"\nWorkspace: https://e2-demo-field-eng.cloud.databricks.com")
print(f"Catalog: {CATALOG}")
print(f"Warehouse: {WAREHOUSE_ID}")

# ============================================================================
# CHECK BRONZE LAYER
# ============================================================================

print("\n" + "="*80)
print("BRONZE LAYER - RAW DATA")
print("="*80)

bronze_tables = {
    "policy_raw": "SELECT COUNT(*) FROM actuary_corpfin.bronze.policy_raw",
    "claims_transactions_raw": "SELECT COUNT(*) FROM actuary_corpfin.bronze.claims_transactions_raw",
    "reinsurance_treaties_raw": "SELECT COUNT(*) FROM actuary_corpfin.bronze.reinsurance_treaties_raw",
    "finance_rates_raw": "SELECT COUNT(*) FROM actuary_corpfin.bronze.finance_rates_raw"
}

bronze_counts = {}
for table, sql in bronze_tables.items():
    result = exec_sql(sql)
    if result and not isinstance(result, str):
        count = result[0][0]
        count = int(count) if count is not None else 0
        bronze_counts[table] = count
        status = "✓" if count > 0 else "✗"
        print(f"  {status} {table}: {count:,} rows")
    else:
        bronze_counts[table] = 0
        print(f"  ✗ {table}: {result}")

# ============================================================================
# CHECK GOLD LAYER
# ============================================================================

print("\n" + "="*80)
print("GOLD LAYER - ANALYTICS TABLES")
print("="*80)

gold_tables = {
    "development_triangles": "SELECT COUNT(*) FROM actuary_corpfin.gold.development_triangles",
    "actual_vs_expected": "SELECT COUNT(*) FROM actuary_corpfin.gold.actual_vs_expected",
    "large_loss_register": "SELECT COUNT(*) FROM actuary_corpfin.gold.large_loss_register",
    "ifrs17_cohorts": "SELECT COUNT(*) FROM actuary_corpfin.gold.ifrs17_cohorts",
    "anomaly_flags": "SELECT COUNT(*) FROM actuary_corpfin.gold.anomaly_flags"
}

gold_counts = {}
for table, sql in gold_tables.items():
    result = exec_sql(sql)
    if result and not isinstance(result, str):
        count = result[0][0]
        count = int(count) if count is not None else 0
        gold_counts[table] = count
        status = "✓" if count > 0 else "✗"
        print(f"  {status} {table}: {count:,} rows")
    else:
        gold_counts[table] = 0
        print(f"  ✗ {table}: {result}")

# ============================================================================
# SAMPLE DATA VALIDATION
# ============================================================================

print("\n" + "="*80)
print("SAMPLE DATA VALIDATION")
print("="*80)

# Check development triangles have proper structure
tri_check = exec_sql("""
    SELECT product, COUNT(DISTINCT accident_year) as years, COUNT(*) as cells
    FROM actuary_corpfin.gold.development_triangles
    GROUP BY product
    ORDER BY product
""")

if tri_check and not isinstance(tri_check, str):
    print("\n  Development Triangles by Product:")
    for row in tri_check:
        print(f"    {row[0]}: {row[1]} accident years, {row[2]} cells")

# Check deterioration flags
det_check = exec_sql("""
    SELECT rag_status, COUNT(*) as count
    FROM actuary_corpfin.gold.actual_vs_expected
    GROUP BY rag_status
    ORDER BY rag_status
""")

if det_check and not isinstance(det_check, str):
    print("\n  Reserve Deterioration RAG Status:")
    for row in det_check:
        print(f"    {row[0]}: {row[1]} monitored cells")

# Check large losses
ll_check = exec_sql("""
    SELECT
        COUNT(*) as total_large_losses,
        SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) as open_claims,
        ROUND(AVG(current_incurred), 0) as avg_incurred
    FROM actuary_corpfin.gold.large_loss_register
""")

if ll_check and not isinstance(ll_check, str):
    row = ll_check[0]
    avg_inc = float(row[2]) if row[2] is not None else 0
    print(f"\n  Large Loss Register:")
    print(f"    Total large losses (>$250K): {row[0]}")
    print(f"    Open claims: {row[1]}")
    print(f"    Average incurred: ${avg_inc:,.0f}")

# Check IFRS 17 cohorts
cohort_check = exec_sql("""
    SELECT
        loss_ratio_flag,
        COUNT(*) as cohort_count,
        ROUND(AVG(loss_ratio), 1) as avg_loss_ratio
    FROM actuary_corpfin.gold.ifrs17_cohorts
    GROUP BY loss_ratio_flag
    ORDER BY loss_ratio_flag
""")

if cohort_check and not isinstance(cohort_check, str):
    print(f"\n  IFRS 17 Cohorts by Loss Ratio Flag:")
    for row in cohort_check:
        avg_lr = float(row[2]) if row[2] is not None else 0
        print(f"    {row[0]}: {row[1]} cohorts, avg LR {avg_lr}%")

# ============================================================================
# FILES CHECK
# ============================================================================

print("\n" + "="*80)
print("DEMO FILES CHECK")
print("="*80)

import os

files_to_check = [
    "DEMO_COMPLETE.md",
    "1_development_triangles.sql",
    "2_reserve_deterioration.sql",
    "3_large_loss_register.sql",
    "4_ifrs17_cohorts.sql",
    "genie_instructions.md",
    "genie_sample_questions.md",
    "actuarial_dlt_serverless.py"
]

for file in files_to_check:
    if os.path.exists(file):
        size = os.path.getsize(file)
        print(f"  ✓ {file} ({size:,} bytes)")
    else:
        print(f"  ✗ {file} MISSING")

# ============================================================================
# DLT PIPELINE CHECK
# ============================================================================

print("\n" + "="*80)
print("DLT PIPELINE STATUS")
print("="*80)

try:
    pipelines = list(w.pipelines.list_pipelines())
    actuarial_pipelines = [p for p in pipelines if 'actuarial_finance' in p.name.lower()]

    if actuarial_pipelines:
        for p in actuarial_pipelines:
            print(f"\n  Pipeline: {p.name}")
            print(f"  ID: {p.pipeline_id}")
            details = w.pipelines.get(pipeline_id=p.pipeline_id)
            print(f"  State: {details.state}")
            print(f"  URL: https://e2-demo-field-eng.cloud.databricks.com/#joblist/pipelines/{p.pipeline_id}")
    else:
        print("  ⚠ No actuarial DLT pipeline found")
except Exception as e:
    print(f"  Error checking pipelines: {e}")

# ============================================================================
# FINAL SUMMARY
# ============================================================================

print("\n" + "="*80)
print("DEMO READINESS SUMMARY")
print("="*80)

all_bronze_loaded = all(count > 0 for count in bronze_counts.values())
all_gold_loaded = all(count > 0 for count in gold_counts.values())
all_files_exist = all(os.path.exists(f) for f in files_to_check)

print(f"\n  Bronze Layer: {'✓ READY' if all_bronze_loaded else '✗ INCOMPLETE'}")
print(f"  Gold Layer: {'✓ READY' if all_gold_loaded else '✗ INCOMPLETE'}")
print(f"  Demo Files: {'✓ READY' if all_files_exist else '✗ INCOMPLETE'}")

if all_bronze_loaded and all_gold_loaded and all_files_exist:
    print(f"\n  🎉 DEMO IS PRODUCTION READY!")
    print(f"\n  Data loaded: ✓")
    print(f"  Analytics tables built: ✓")
    print(f"  Dashboard SQL queries ready: ✓")
    print(f"  Genie documentation ready: ✓")
    print(f"  Documentation complete: ✓")

    print(f"\n  MANUAL STEPS REMAINING:")
    print(f"  1. Create Genie Space at: https://e2-demo-field-eng.cloud.databricks.com/genie")
    print(f"  2. Create Lakeview Dashboard at: https://e2-demo-field-eng.cloud.databricks.com/sql/dashboards")
    print(f"  3. (Optional) Run DLT pipeline for live data flow demo")
else:
    print(f"\n  ⚠ DEMO INCOMPLETE - Review errors above")

print("\n" + "="*80)
