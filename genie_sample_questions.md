
# Genie Sample Questions with SQL Answers

These are pre-tested questions you can ask in the Genie space:

## Question 1: Triangle Summary
**Ask:** "Show me the development triangles for motor insurance in NSW"

**SQL Answer:**
```sql
SELECT
    accident_year,
    dev_period,
    cumulative_incurred,
    cumulative_paid,
    claim_count
FROM actuary_corpfin.gold.development_triangles
WHERE product = 'motor' AND state = 'NSW'
ORDER BY accident_year, dev_period
```

## Question 2: Reserve Deterioration
**Ask:** "Which triangles are showing deterioration above 10%?"

**SQL Answer:**
```sql
SELECT
    product,
    state,
    accident_year,
    variance_pct,
    rag_status
FROM actuary_corpfin.gold.actual_vs_expected
WHERE variance_pct > 10
ORDER BY variance_pct DESC
```

## Question 3: Large Losses
**Ask:** "What are the top 10 largest claims?"

**SQL Answer:**
```sql
SELECT
    claim_id,
    product,
    state,
    peril,
    current_incurred,
    status
FROM actuary_corpfin.gold.large_loss_register
ORDER BY current_incurred DESC
LIMIT 10
```

## Question 4: IFRS 17 Problem Cohorts
**Ask:** "Which cohorts have loss ratios above 80%?"

**SQL Answer:**
```sql
SELECT
    cohort_id,
    policy_count,
    loss_ratio,
    loss_ratio_flag
FROM actuary_corpfin.gold.ifrs17_cohorts
WHERE loss_ratio > 80
ORDER BY loss_ratio DESC
```

## Question 5: Product Comparison
**Ask:** "Compare total incurred amounts by product"

**SQL Answer:**
```sql
SELECT
    product,
    SUM(cumulative_incurred) as total_incurred,
    AVG(cumulative_incurred) as avg_incurred,
    COUNT(DISTINCT concat(accident_year, dev_period)) as triangle_cells
FROM actuary_corpfin.gold.development_triangles
GROUP BY product
ORDER BY total_incurred DESC
```

## Question 6: State Analysis
**Ask:** "Which state has the highest loss ratio?"

**SQL Answer:**
```sql
SELECT
    SUBSTRING(cohort_id, POSITION('_' IN cohort_id) + 1, 3) as state,
    AVG(loss_ratio) as avg_loss_ratio,
    SUM(gross_written_premium) as total_gwp
FROM actuary_corpfin.gold.ifrs17_cohorts
GROUP BY SUBSTRING(cohort_id, POSITION('_' IN cohort_id) + 1, 3)
ORDER BY avg_loss_ratio DESC
```

## Question 7: Development Speed
**Ask:** "How fast do claims develop to 80% of ultimate for each product?"

**SQL Answer:**
```sql
WITH dev_pct AS (
    SELECT
        product,
        accident_year,
        dev_period,
        cumulative_incurred,
        MAX(cumulative_incurred) OVER (PARTITION BY product, accident_year) as ultimate
    FROM actuary_corpfin.gold.development_triangles
)
SELECT
    product,
    MIN(dev_period) as months_to_80pct
FROM dev_pct
WHERE cumulative_incurred >= ultimate * 0.8
GROUP BY product
```

## Question 8: Open vs Closed
**Ask:** "What percentage of large losses are still open?"

**SQL Answer:**
```sql
SELECT
    COUNT(CASE WHEN status = 'open' THEN 1 END) * 100.0 / COUNT(*) as pct_open,
    COUNT(*) as total_large_losses,
    COUNT(CASE WHEN status = 'open' THEN 1 END) as open_count,
    COUNT(CASE WHEN status = 'closed' THEN 1 END) as closed_count
FROM actuary_corpfin.gold.large_loss_register
```

## Question 9: Peril Analysis
**Ask:** "Which perils have the highest average incurred amount?"

**SQL Answer:**
```sql
SELECT
    peril,
    COUNT(DISTINCT claim_id) as claim_count,
    AVG(current_incurred) as avg_incurred,
    MAX(current_incurred) as max_incurred
FROM actuary_corpfin.gold.large_loss_register
GROUP BY peril
ORDER BY avg_incurred DESC
```

## Question 10: Quarterly Trends
**Ask:** "Show loss ratio trends by quarter for 2024 cohorts"

**SQL Answer:**
```sql
SELECT
    cohort_inception_quarter,
    AVG(loss_ratio) as avg_loss_ratio,
    SUM(policy_count) as total_policies,
    COUNT(*) as cohort_count
FROM actuary_corpfin.gold.ifrs17_cohorts
WHERE cohort_inception_year = 2024
GROUP BY cohort_inception_quarter
ORDER BY cohort_inception_quarter
```
