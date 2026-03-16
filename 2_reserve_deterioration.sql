
-- Reserve Deterioration Monitor
-- Red flags for reserve adequacy

SELECT
    product,
    state,
    accident_year,
    dev_period,
    actual_factor,
    expected_factor,
    variance_pct,
    rag_status,
    CASE
        WHEN variance_pct >= 15 THEN '🔴 CRITICAL'
        WHEN variance_pct >= 5 THEN '🟡 WATCH'
        ELSE '🟢 OK'
    END as alert
FROM actuary_corpfin.gold.actual_vs_expected
WHERE accident_year >= 2022
ORDER BY variance_pct DESC
LIMIT 50
