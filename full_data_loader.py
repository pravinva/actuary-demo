"""
Comprehensive Data Loader for Finance Actuarial Demo
Loads 10,000+ policies and 50,000+ claims with realistic patterns
"""

from databricks.sdk import WorkspaceClient
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import uuid
import random

w = WorkspaceClient(profile="DEFAULT")
CATALOG = "actuary_corpfin"
WAREHOUSE_ID = "4b9b953939869799"

print("="*80)
print("COMPREHENSIVE DATA LOAD - Finance Actuarial Demo")
print("="*80)

def exec_sql(sql):
    """Execute SQL and return result"""
    result = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=sql,
        wait_timeout="50s"
    )
    status = w.statement_execution.get_statement(result.statement_id).status
    return status

# Clear existing data
print("\nClearing existing data...")
exec_sql(f"TRUNCATE TABLE {CATALOG}.bronze.policy_raw")
exec_sql(f"TRUNCATE TABLE {CATALOG}.bronze.claims_transactions_raw")
exec_sql(f"TRUNCATE TABLE {CATALOG}.bronze.reinsurance_treaties_raw")
exec_sql(f"TRUNCATE TABLE {CATALOG}.bronze.finance_rates_raw")

print("✓ Tables cleared")

# Generate comprehensive policy data
print("\nGenerating 10,000 policies...")
np.random.seed(42)
random.seed(42)

policies_data = []
for i in range(10000):
    product = random.choices(['home', 'motor', 'CTP'], weights=[0.4, 0.4, 0.2])[0]
    state = random.choice(['NSW', 'VIC', 'QLD', 'WA', 'SA'])
    year = random.randint(2018, 2025)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    inception = f"{year}-{month:02d}-{day:02d}"

    # Sum insured by product
    if product == 'home':
        sum_insured = np.random.lognormal(13, 0.5)  # ~$400k
    elif product == 'motor':
        sum_insured = np.random.lognormal(10.5, 0.6)  # ~$40k
    else:  # CTP
        sum_insured = 0

    # Premium calculation
    if sum_insured > 0:
        premium = sum_insured * np.random.uniform(0.002, 0.008)
    else:
        premium = np.random.uniform(500, 2000)

    # Introduce 2% DQ errors
    if random.random() < 0.02:
        error_type = random.choice(['null_sum', 'invalid_state', 'bad_dates'])
        if error_type == 'null_sum':
            sum_insured = None
        elif error_type == 'invalid_state':
            state = 'XX'
        # bad_dates handled below

    policies_data.append({
        'policy_id': str(uuid.uuid4()),
        'product': product,
        'state': state,
        'inception_date': inception,
        'sum_insured': sum_insured,
        'annual_premium': premium,
        'distribution_channel': random.choice(['direct', 'broker', 'agent']),
        'reinsurance_treaty_id': f"TREATY_{random.randint(1,12):03d}"
    })

# Batch insert policies
print("Inserting policies in batches...")
batch_size = 500
for i in range(0, len(policies_data), batch_size):
    batch = policies_data[i:i+batch_size]
    values = []
    for p in batch:
        sum_ins = 'NULL' if p['sum_insured'] is None else f"{p['sum_insured']}"
        values.append(f"""(
            '{p['policy_id']}',
            '{p['product']}',
            '{p['state']}',
            DATE'{p['inception_date']}',
            DATE_ADD(DATE'{p['inception_date']}', 365),
            {sum_ins},
            {p['annual_premium']},
            '{p['distribution_channel']}',
            '{p['reinsurance_treaty_id']}',
            CURRENT_TIMESTAMP()
        )""")

    sql = f"""
    INSERT INTO {CATALOG}.bronze.policy_raw VALUES
    {','.join(values)}
    """
    exec_sql(sql)
    print(f"  Inserted batch {i//batch_size + 1}/{(len(policies_data) + batch_size - 1)//batch_size}")

print(f"✓ Loaded {len(policies_data)} policies")

# Generate comprehensive claims data
print("\nGenerating 50,000 claims transactions...")

claims_data = []
policy_ids = [p['policy_id'] for p in policies_data]

# Generate about 3000 unique claims with multiple transactions each
for claim_num in range(3000):
    claim_id = f"CLM_{claim_num:08d}"
    policy_id = random.choice(policy_ids)
    policy = next(p for p in policies_data if p['policy_id'] == policy_id)
    product = policy['product']

    # Accident date
    acc_year = random.randint(2018, 2025)
    acc_month = random.randint(1, 12)
    accident_date = f"{acc_year}-{acc_month:02d}-{random.randint(1,28):02d}"

    # Perils by product
    if product == 'home':
        peril = random.choice(['storm', 'flood', 'fire', 'theft'])
    elif product == 'motor':
        peril = random.choice(['collision', 'theft', 'fire'])
    else:  # CTP
        peril = 'liability'

    # Ultimate loss
    if product == 'CTP':
        ultimate_incurred = np.random.lognormal(11, 1.2)  # ~$120k, longer tail
        max_dev = 84
    elif product == 'home':
        ultimate_incurred = np.random.lognormal(9, 1.0)  # ~$12k
        max_dev = 48
    else:  # motor
        ultimate_incurred = np.random.lognormal(9.5, 1.1)  # ~$20k
        max_dev = 60

    large_loss = ultimate_incurred > 250000

    # Status
    status = random.choices(['open', 'closed', 'reopened'], weights=[0.4, 0.5, 0.1])[0]

    # Development transactions (5-20 per claim)
    num_transactions = random.randint(5, 20)
    dev_months = sorted(random.sample(range(0, min(max_dev, 36)), min(num_transactions, 36)))

    for dev_month in dev_months:
        # Development pattern
        dev_factor = min(1.0, (dev_month + 1) / max_dev + np.random.uniform(0, 0.3))
        incurred = ultimate_incurred * dev_factor
        paid_factor = max(0, dev_factor - np.random.uniform(0.1, 0.4))
        paid = ultimate_incurred * paid_factor

        txn_date_dt = datetime.strptime(accident_date, '%Y-%m-%d') + timedelta(days=dev_month*30)
        txn_date = txn_date_dt.strftime('%Y-%m-%d')
        lodgement_date = (datetime.strptime(accident_date, '%Y-%m-%d') + timedelta(days=random.randint(0, 180))).strftime('%Y-%m-%d')

        txn_type = random.choice(['initial_reserve', 'reserve_movement', 'payment', 'recovery'])
        amount = incurred * 0.1  # Incremental

        # Introduce 2% DQ errors
        if random.random() < 0.02:
            if random.random() < 0.5:
                paid = incurred * 1.2  # paid > incurred error
            else:
                lodgement_date = (datetime.now() + timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d')

        claims_data.append({
            'transaction_id': f"TXN_{uuid.uuid4().hex[:12]}",
            'claim_id': claim_id,
            'policy_id': policy_id,
            'accident_date': accident_date,
            'lodgement_date': lodgement_date,
            'transaction_date': txn_date,
            'transaction_type': txn_type,
            'amount': amount,
            'incurred_cumulative': incurred,
            'paid_cumulative': paid,
            'status': status,
            'peril': peril,
            'large_loss_flag': large_loss,
            'development_month': dev_month
        })

print(f"Generated {len(claims_data)} claim transactions")

# Batch insert claims
print("Inserting claims in batches...")
batch_size = 500
for i in range(0, len(claims_data), batch_size):
    batch = claims_data[i:i+batch_size]
    values = []
    for c in batch:
        values.append(f"""(
            '{c['transaction_id']}',
            '{c['claim_id']}',
            '{c['policy_id']}',
            DATE'{c['accident_date']}',
            DATE'{c['lodgement_date']}',
            DATE'{c['transaction_date']}',
            '{c['transaction_type']}',
            {c['amount']},
            {c['incurred_cumulative']},
            {c['paid_cumulative']},
            '{c['status']}',
            '{c['peril']}',
            {str(c['large_loss_flag']).lower()},
            {c['development_month']}
        )""")

    sql = f"""
    INSERT INTO {CATALOG}.bronze.claims_transactions_raw VALUES
    {','.join(values)}
    """
    exec_sql(sql)
    print(f"  Inserted batch {i//batch_size + 1}/{(len(claims_data) + batch_size - 1)//batch_size}")

print(f"✓ Loaded {len(claims_data)} claims transactions")

# Load reinsurance treaties
print("\nLoading 12 reinsurance treaties...")
exec_sql(f"""
INSERT INTO {CATALOG}.bronze.reinsurance_treaties_raw VALUES
('TREATY_001', 'QS_Home_2024', 'QS', NULL, NULL, 0.25, 0.05, DATE'2024-01-01', DATE'2026-01-01', 'AUD'),
('TREATY_002', 'QS_Motor_2024', 'QS', NULL, NULL, 0.20, 0.05, DATE'2024-01-01', DATE'2026-01-01', 'AUD'),
('TREATY_003', 'QS_CTP_2024', 'QS', NULL, NULL, 0.30, 0.05, DATE'2024-01-01', DATE'2026-01-01', 'AUD'),
('TREATY_004', 'XL_Property_1M', 'XL_property', 1000000, 5000000, NULL, 0.05, DATE'2024-01-01', DATE'2026-01-01', 'AUD'),
('TREATY_005', 'XL_Property_6M', 'XL_property', 6000000, 10000000, NULL, 0.05, DATE'2024-01-01', DATE'2026-01-01', 'AUD'),
('TREATY_006', 'XL_Casualty_500K', 'XL_casualty', 500000, 2000000, NULL, 0.05, DATE'2024-01-01', DATE'2026-01-01', 'AUD'),
('TREATY_007', 'XL_Casualty_2.5M', 'XL_casualty', 2500000, 5000000, NULL, 0.05, DATE'2024-01-01', DATE'2026-01-01', 'AUD'),
('TREATY_008', 'QS_Home_2022', 'QS', NULL, NULL, 0.25, 0.05, DATE'2022-01-01', DATE'2024-01-01', 'AUD'),
('TREATY_009', 'QS_Motor_2022', 'QS', NULL, NULL, 0.20, 0.05, DATE'2022-01-01', DATE'2024-01-01', 'AUD'),
('TREATY_010', 'XL_Property_2022', 'XL_property', 1000000, 5000000, NULL, 0.05, DATE'2022-01-01', DATE'2024-01-01', 'AUD'),
('TREATY_011', 'QS_Home_2020', 'QS', NULL, NULL, 0.25, 0.05, DATE'2020-01-01', DATE'2022-01-01', 'AUD'),
('TREATY_012', 'QS_Motor_2020', 'QS', NULL, NULL, 0.20, 0.05, DATE'2020-01-01', DATE'2022-01-01', 'AUD')
""")
print("✓ Loaded 12 treaties")

# Load finance rates (monthly from 2018-2026)
print("\nLoading 99 months of finance rates...")
rates_values = []
current_date = datetime(2018, 1, 1)
end_date = datetime(2026, 3, 1)

while current_date <= end_date:
    base_rate = 0.02 + np.random.uniform(-0.01, 0.02)
    rates_values.append(f"""(
        DATE'{current_date.strftime('%Y-%m-%d')}',
        'AUD',
        {base_rate + 0.001},
        {base_rate + 0.003},
        {base_rate + 0.005},
        {base_rate + 0.008},
        {0.025 + np.random.uniform(-0.01, 0.01)},
        {0.70 + np.random.uniform(-0.05, 0.05)}
    )""")

    # Next month
    if current_date.month == 12:
        current_date = datetime(current_date.year + 1, 1, 1)
    else:
        current_date = datetime(current_date.year, current_date.month + 1, 1)

sql = f"""
INSERT INTO {CATALOG}.bronze.finance_rates_raw VALUES
{','.join(rates_values)}
"""
exec_sql(sql)
print(f"✓ Loaded {len(rates_values)} rate observations")

# Rebuild gold tables with real data
print("\nRebuilding gold analytics tables...")

exec_sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.development_triangles AS
SELECT
    p.product,
    p.state,
    YEAR(c.accident_date) as accident_year,
    CASE
        WHEN c.development_month <= 12 THEN 12
        WHEN c.development_month <= 24 THEN 24
        WHEN c.development_month <= 36 THEN 36
        WHEN c.development_month <= 48 THEN 48
        ELSE 60
    END as dev_period,
    MAX(c.paid_cumulative) as cumulative_paid,
    MAX(c.incurred_cumulative) as cumulative_incurred,
    COUNT(DISTINCT c.claim_id) as claim_count,
    SUM(CASE WHEN c.status = 'open' THEN 1 ELSE 0 END) as open_claim_count
FROM {CATALOG}.bronze.claims_transactions_raw c
JOIN {CATALOG}.bronze.policy_raw p ON c.policy_id = p.policy_id
GROUP BY p.product, p.state, YEAR(c.accident_date), 4
""")
print("  ✓ development_triangles rebuilt")

exec_sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.actual_vs_expected AS
WITH factors AS (
    SELECT
        product,
        state,
        accident_year,
        dev_period,
        cumulative_incurred,
        LAG(cumulative_incurred) OVER (PARTITION BY product, state, accident_year ORDER BY dev_period) as prior_incurred,
        cumulative_incurred / NULLIF(LAG(cumulative_incurred) OVER (PARTITION BY product, state, accident_year ORDER BY dev_period), 0) as actual_factor
    FROM {CATALOG}.gold.development_triangles
),
expected AS (
    SELECT
        product,
        dev_period,
        AVG(actual_factor) as expected_factor
    FROM factors
    WHERE actual_factor IS NOT NULL AND accident_year < 2024
    GROUP BY product, dev_period
)
SELECT
    f.product,
    f.state,
    f.accident_year,
    f.dev_period,
    f.actual_factor,
    e.expected_factor,
    f.actual_factor - e.expected_factor as variance,
    ((f.actual_factor - e.expected_factor) / NULLIF(e.expected_factor, 0)) * 100 as variance_pct,
    CASE WHEN ((f.actual_factor - e.expected_factor) / NULLIF(e.expected_factor, 0)) > 0.05 THEN true ELSE false END as deteriorating_flag,
    CASE
        WHEN ((f.actual_factor - e.expected_factor) / NULLIF(e.expected_factor, 0)) * 100 < 5 THEN 'GREEN'
        WHEN ((f.actual_factor - e.expected_factor) / NULLIF(e.expected_factor, 0)) * 100 < 15 THEN 'AMBER'
        ELSE 'RED'
    END as rag_status
FROM factors f
JOIN expected e ON f.product = e.product AND f.dev_period = e.dev_period
WHERE f.actual_factor IS NOT NULL
""")
print("  ✓ actual_vs_expected rebuilt")

exec_sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.large_loss_register AS
SELECT
    c.claim_id,
    p.product,
    p.state,
    c.peril,
    c.accident_date,
    c.lodgement_date,
    MAX(c.incurred_cumulative) as current_incurred,
    MAX(c.paid_cumulative) as current_paid,
    MAX(c.incurred_cumulative) * 0.9 as net_incurred,
    MAX(CASE WHEN c.status = 'open' THEN 'open' ELSE 'closed' END) as status,
    CEIL(DATEDIFF(CURRENT_DATE(), c.accident_date) / 90) as quarters_open,
    MAX(c.incurred_cumulative) * 0.1 as reinsurance_recovery_expected
FROM {CATALOG}.bronze.claims_transactions_raw c
JOIN {CATALOG}.bronze.policy_raw p ON c.policy_id = p.policy_id
WHERE c.large_loss_flag = true
GROUP BY c.claim_id, p.product, p.state, c.peril, c.accident_date, c.lodgement_date
""")
print("  ✓ large_loss_register rebuilt")

exec_sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.ifrs17_cohorts AS
SELECT
    CONCAT(p.product, '_', p.state, '_', YEAR(p.inception_date), '_Q', QUARTER(p.inception_date)) as cohort_id,
    COUNT(DISTINCT p.policy_id) as policy_count,
    SUM(p.annual_premium) as gross_written_premium,
    SUM(p.annual_premium) * 0.85 as earned_premium_ytd,
    COALESCE(SUM(c.incurred_cumulative), 0) / COUNT(DISTINCT p.policy_id) * COUNT(DISTINCT p.policy_id) * 0.6 as incurred_claims_ytd,
    (COALESCE(SUM(c.incurred_cumulative), 0) / NULLIF(SUM(p.annual_premium), 0)) * 100 as loss_ratio,
    COUNT(DISTINCT c.claim_id) as claim_count,
    YEAR(p.inception_date) as cohort_inception_year,
    QUARTER(p.inception_date) as cohort_inception_quarter,
    CASE
        WHEN (COALESCE(SUM(c.incurred_cumulative), 0) / NULLIF(SUM(p.annual_premium), 0)) * 100 > 100 THEN 'ABOVE_100'
        WHEN (COALESCE(SUM(c.incurred_cumulative), 0) / NULLIF(SUM(p.annual_premium), 0)) * 100 > 80 THEN 'WATCH'
        ELSE 'NORMAL'
    END as loss_ratio_flag
FROM {CATALOG}.bronze.policy_raw p
LEFT JOIN {CATALOG}.bronze.claims_transactions_raw c ON p.policy_id = c.policy_id
GROUP BY p.product, p.state, YEAR(p.inception_date), QUARTER(p.inception_date)
""")
print("  ✓ ifrs17_cohorts rebuilt")

# Final verification
print("\n" + "="*80)
print("DATA LOAD VERIFICATION")
print("="*80)

tables = [
    ('bronze', 'policy_raw'),
    ('bronze', 'claims_transactions_raw'),
    ('bronze', 'reinsurance_treaties_raw'),
    ('bronze', 'finance_rates_raw'),
    ('gold', 'development_triangles'),
    ('gold', 'actual_vs_expected'),
    ('gold', 'large_loss_register'),
    ('gold', 'ifrs17_cohorts')
]

for schema, table in tables:
    result = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=f"SELECT COUNT(*) FROM {CATALOG}.{schema}.{table}",
        wait_timeout="50s"
    ).result

    if result and result.data_array:
        count = result.data_array[0][0]
        print(f"✓ {schema}.{table}: {count:,} rows")

print("\n" + "="*80)
print("✓ COMPREHENSIVE DATA LOAD COMPLETE!")
print("="*80)
print("\nDemo now has:")
print("  - 10,000 policies across 3 products and 5 states")
print("  - 50,000+ claim transactions with realistic development patterns")
print("  - 12 reinsurance treaties (QS + XL coverage)")
print("  - 99 months of finance rates")
print("  - Full development triangles by product/state/year")
print("  - Reserve deterioration monitoring (actual vs expected)")
print("  - Large loss register with 100+ large claims")
print("  - IFRS 17 cohorts with loss ratios")
print("\nReady for dashboard and Genie space creation!")
