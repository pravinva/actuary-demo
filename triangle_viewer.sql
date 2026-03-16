
-- Development Triangle Viewer
SELECT * FROM actuary_corpfin.gold.development_triangles
WHERE product = :product
  AND state = :state
ORDER BY accident_year, dev_period
