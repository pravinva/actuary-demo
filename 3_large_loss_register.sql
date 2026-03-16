
-- Large Loss Register
-- Claims exceeding $250K

SELECT
    claim_id,
    product,
    state,
    peril,
    accident_date,
    current_incurred,
    current_paid,
    net_incurred,
    status,
    quarters_open,
    reinsurance_recovery_expected
FROM actuary_corpfin.gold.large_loss_register
WHERE current_incurred >= 250000
ORDER BY current_incurred DESC
LIMIT 100
