
-- IFRS 17 Cohort Summary
-- Regulatory reporting by inception cohort

SELECT
    cohort_id,
    policy_count,
    gross_written_premium,
    earned_premium_ytd,
    incurred_claims_ytd,
    ROUND(loss_ratio, 2) as loss_ratio_pct,
    claim_count,
    cohort_inception_year,
    cohort_inception_quarter,
    loss_ratio_flag,
    CASE
        WHEN loss_ratio_flag = 'ABOVE_100' THEN '🔴 Action Required'
        WHEN loss_ratio_flag = 'WATCH' THEN '🟡 Monitor'
        ELSE '🟢 Healthy'
    END as status
FROM actuary_corpfin.gold.ifrs17_cohorts
ORDER BY loss_ratio DESC
LIMIT 50
