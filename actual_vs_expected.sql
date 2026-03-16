
-- Reserve Deterioration Monitor
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
        WHEN variance_pct < 5 THEN 'GREEN'
        WHEN variance_pct < 15 THEN 'AMBER'
        ELSE 'RED'
    END as alert_level
FROM actuary_corpfin.gold.actual_vs_expected
WHERE accident_year >= 2022
ORDER BY variance_pct DESC
