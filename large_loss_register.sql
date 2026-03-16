
-- Large Loss Register
SELECT * FROM actuary_corpfin.gold.large_loss_register
WHERE current_incurred >= :min_incurred
  AND status = :status_filter
ORDER BY current_incurred DESC
