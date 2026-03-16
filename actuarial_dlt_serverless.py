# Databricks notebook source
"""
Finance Actuarial DLT Pipeline - Serverless Configuration
Bronze → Silver → Gold with DQ Expectations

Catalog: actuary_corpfin
Target: silver, gold schemas
Serverless: Enabled
"""

import dlt
from pyspark.sql import functions as F

# ============================================================================
# SILVER LAYER
# ============================================================================

@dlt.table(
    name="policy_clean",
    comment="Cleaned policy data with DQ filters applied",
    table_properties={
        "quality": "silver",
        "layer": "silver"
    }
)
@dlt.expect_or_drop("valid_dates", "expiry_date > inception_date")
@dlt.expect_or_drop("valid_state", "state IN ('NSW','VIC','QLD','WA','SA','TAS','ACT','NT')")
@dlt.expect_or_drop("positive_premium", "annual_premium > 0")
def policy_clean():
    return spark.sql("""
        SELECT
            policy_id,
            UPPER(product) as product,
            UPPER(state) as state,
            inception_date,
            expiry_date,
            sum_insured,
            annual_premium,
            distribution_channel,
            reinsurance_treaty_id
        FROM actuary_corpfin.bronze.policy_raw
        WHERE sum_insured IS NOT NULL OR product = 'CTP'
    """)


@dlt.table(
    name="claims_clean",
    comment="Cleaned claims transactions with DQ filters",
    table_properties={
        "quality": "silver",
        "layer": "silver"
    }
)
@dlt.expect_or_drop("incurred_gte_paid", "incurred_cumulative >= paid_cumulative")
@dlt.expect_or_drop("valid_lodgement", "lodgement_date >= accident_date")
@dlt.expect_or_drop("valid_status", "status IN ('open','closed','reopened')")
def claims_clean():
    return spark.sql("""
        SELECT
            c.*,
            p.product,
            p.state
        FROM actuary_corpfin.bronze.claims_transactions_raw c
        LEFT JOIN actuary_corpfin.bronze.policy_raw p
            ON c.policy_id = p.policy_id
        WHERE lodgement_date <= CURRENT_DATE()
    """)


@dlt.table(
    name="dq_quarantine",
    comment="Quarantined records with data quality issues"
)
def dq_quarantine():
    return spark.sql("""
        SELECT
            'future_lodgement' as error_type,
            claim_id as entity_id,
            CONCAT('Lodgement date ', lodgement_date, ' is in the future') as error_description,
            CURRENT_TIMESTAMP() as quarantine_timestamp
        FROM actuary_corpfin.bronze.claims_transactions_raw
        WHERE lodgement_date > CURRENT_DATE()

        UNION ALL

        SELECT
            'paid_exceeds_incurred' as error_type,
            claim_id as entity_id,
            CONCAT('Paid (', paid_cumulative, ') exceeds incurred (', incurred_cumulative, ')') as error_description,
            CURRENT_TIMESTAMP() as quarantine_timestamp
        FROM actuary_corpfin.bronze.claims_transactions_raw
        WHERE paid_cumulative > incurred_cumulative
    """)


# ============================================================================
# GOLD LAYER - ANALYTICS
# ============================================================================

@dlt.table(
    name="development_triangles",
    comment="Claims development triangles by product/state/year",
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "use_case": "reserve_analysis"
    }
)
def development_triangles():
    return spark.sql("""
        SELECT
            product,
            state,
            YEAR(accident_date) as accident_year,
            CASE
                WHEN development_month <= 12 THEN 12
                WHEN development_month <= 24 THEN 24
                WHEN development_month <= 36 THEN 36
                WHEN development_month <= 48 THEN 48
                WHEN development_month <= 60 THEN 60
                ELSE 84
            END as dev_period,
            MAX(paid_cumulative) as cumulative_paid,
            MAX(incurred_cumulative) as cumulative_incurred,
            COUNT(DISTINCT claim_id) as claim_count,
            SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) as open_claim_count
        FROM LIVE.claims_clean
        GROUP BY product, state, YEAR(accident_date), 4
    """)


@dlt.table(
    name="actual_vs_expected",
    comment="Reserve deterioration monitoring - actual vs expected development factors",
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "use_case": "reserve_monitoring"
    }
)
def actual_vs_expected():
    return spark.sql("""
        WITH factors AS (
            SELECT
                product,
                state,
                accident_year,
                dev_period,
                cumulative_incurred,
                LAG(cumulative_incurred) OVER (
                    PARTITION BY product, state, accident_year
                    ORDER BY dev_period
                ) as prior_incurred,
                cumulative_incurred / NULLIF(
                    LAG(cumulative_incurred) OVER (
                        PARTITION BY product, state, accident_year
                        ORDER BY dev_period
                    ), 0
                ) as actual_factor
            FROM LIVE.development_triangles
        ),
        expected AS (
            SELECT
                product,
                dev_period,
                AVG(actual_factor) as expected_factor
            FROM factors
            WHERE actual_factor IS NOT NULL
                AND accident_year < YEAR(CURRENT_DATE()) - 1
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
            CASE
                WHEN ((f.actual_factor - e.expected_factor) / NULLIF(e.expected_factor, 0)) > 0.05
                THEN true
                ELSE false
            END as deteriorating_flag,
            CASE
                WHEN ((f.actual_factor - e.expected_factor) / NULLIF(e.expected_factor, 0)) * 100 < 5
                    THEN 'GREEN'
                WHEN ((f.actual_factor - e.expected_factor) / NULLIF(e.expected_factor, 0)) * 100 < 15
                    THEN 'AMBER'
                ELSE 'RED'
            END as rag_status
        FROM factors f
        JOIN expected e
            ON f.product = e.product
            AND f.dev_period = e.dev_period
        WHERE f.actual_factor IS NOT NULL
    """)


@dlt.table(
    name="large_loss_register",
    comment="Claims exceeding $250K - board reporting",
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "use_case": "large_loss_monitoring"
    }
)
def large_loss_register():
    return spark.sql("""
        SELECT
            claim_id,
            product,
            state,
            peril,
            accident_date,
            lodgement_date,
            MAX(incurred_cumulative) as current_incurred,
            MAX(paid_cumulative) as current_paid,
            MAX(incurred_cumulative) * 0.9 as net_incurred,
            MAX(CASE WHEN status = 'open' THEN 'open' ELSE 'closed' END) as status,
            CEIL(DATEDIFF(CURRENT_DATE(), accident_date) / 90) as quarters_open,
            MAX(incurred_cumulative) * 0.1 as reinsurance_recovery_expected
        FROM LIVE.claims_clean
        WHERE large_loss_flag = true
        GROUP BY claim_id, product, state, peril, accident_date, lodgement_date
    """)


@dlt.table(
    name="ifrs17_cohorts",
    comment="IFRS 17 policy cohorts with loss ratios",
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "use_case": "regulatory_reporting"
    }
)
def ifrs17_cohorts():
    return spark.sql("""
        SELECT
            CONCAT(
                p.product, '_',
                p.state, '_',
                YEAR(p.inception_date), '_Q',
                QUARTER(p.inception_date)
            ) as cohort_id,
            COUNT(DISTINCT p.policy_id) as policy_count,
            SUM(p.annual_premium) as gross_written_premium,
            SUM(p.annual_premium) * 0.85 as earned_premium_ytd,
            COALESCE(
                SUM(c.incurred_cumulative), 0
            ) / COUNT(DISTINCT p.policy_id) * COUNT(DISTINCT p.policy_id) * 0.6 as incurred_claims_ytd,
            (COALESCE(SUM(c.incurred_cumulative), 0) / NULLIF(SUM(p.annual_premium), 0)) * 100 as loss_ratio,
            COUNT(DISTINCT c.claim_id) as claim_count,
            YEAR(p.inception_date) as cohort_inception_year,
            QUARTER(p.inception_date) as cohort_inception_quarter,
            CASE
                WHEN (COALESCE(SUM(c.incurred_cumulative), 0) / NULLIF(SUM(p.annual_premium), 0)) * 100 > 100
                    THEN 'ABOVE_100'
                WHEN (COALESCE(SUM(c.incurred_cumulative), 0) / NULLIF(SUM(p.annual_premium), 0)) * 100 > 80
                    THEN 'WATCH'
                ELSE 'NORMAL'
            END as loss_ratio_flag
        FROM actuary_corpfin.bronze.policy_raw p
        LEFT JOIN LIVE.claims_clean c
            ON p.policy_id = c.policy_id
        GROUP BY p.product, p.state, YEAR(p.inception_date), QUARTER(p.inception_date)
    """)


@dlt.table(
    name="anomaly_flags",
    comment="ML-based anomaly detection flags",
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "use_case": "anomaly_detection"
    }
)
def anomaly_flags():
    return spark.sql("""
        SELECT
            'TRIANGLE_DETERIORATION' as flag_type,
            CONCAT(product, '_', state, '_', accident_year, '_', dev_period) as entity_id,
            CONCAT(
                'Actual development factor ',
                ROUND(actual_factor, 3),
                ' exceeds expected ',
                ROUND(expected_factor, 3),
                ' by ',
                ROUND(variance_pct, 1),
                '%'
            ) as flag_reason,
            CASE
                WHEN variance_pct > 20 THEN 'HIGH'
                WHEN variance_pct > 10 THEN 'MEDIUM'
                ELSE 'LOW'
            END as severity,
            CURRENT_TIMESTAMP() as flag_timestamp,
            'statistical_v1' as model_version
        FROM LIVE.actual_vs_expected
        WHERE deteriorating_flag = true
    """)
