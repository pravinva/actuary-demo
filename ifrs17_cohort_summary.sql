
-- IFRS 17 Cohort Summary
SELECT
    cohort_id,
    policy_count,
    gross_written_premium,
    earned_premium_ytd,
    incurred_claims_ytd,
    loss_ratio,
    loss_ratio_flag
FROM actuary_corpfin.gold.ifrs17_cohorts
ORDER BY loss_ratio DESC
