
-- Development Triangles
-- Shows reserve development by product, state, and accident year

SELECT
    product,
    state,
    accident_year,
    dev_period,
    cumulative_incurred,
    cumulative_paid,
    claim_count,
    open_claim_count
FROM actuary_corpfin.gold.development_triangles
ORDER BY product, state, accident_year, dev_period
