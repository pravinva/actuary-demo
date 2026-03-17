
# Finance Actuarial Genie Space

**Space Name:** Finance Actuarial Analytics
**Space ID:** 01f1225bb489165aa883cbbb7eed64e4
**URL:** https://e2-demo-field-eng.cloud.databricks.com/explore/genie/01f1225bb489165aa883cbbb7eed64e4
**Warehouse:** 4b9b953939869799

This Genie space provides natural language access to actuarial analytics for insurance portfolio monitoring.

## Available Data

**Catalog:** `actuary_corpfin`

**Tables:**
- `gold.development_triangles` - Claims development by product/state/year/period
- `gold.actual_vs_expected` - Reserve deterioration monitoring
- `gold.large_loss_register` - Claims exceeding $250K
- `gold.ifrs17_cohorts` - IFRS 17 regulatory cohorts
- `bronze.policy_raw` - Policy details
- `bronze.claims_transactions_raw` - Claim transaction history

## Key Metrics

- **Development Period (dev_period)**: Months since accident (12, 24, 36, 48, 60)
- **Cumulative Incurred**: Total reserve including case reserves
- **Cumulative Paid**: Total payments to date
- **Loss Ratio**: Incurred claims / Earned premium × 100
- **Variance %**: (Actual - Expected) / Expected × 100

## Sample Questions to Ask

### Development Triangles
1. "Show me the development triangles for motor insurance in NSW"
2. "What are the cumulative incurred amounts by accident year for home insurance?"
3. "Compare development patterns between motor and CTP products"
4. "Show claim counts by development period for 2024 accident year"

### Reserve Deterioration
5. "Which triangles are showing deterioration above 10%?"
6. "Show me all RED status reserves"
7. "What is the average variance percentage by product?"
8. "List accident years with deteriorating reserves for motor insurance"

### Large Losses
9. "What are the top 10 largest claims?"
10. "Show me all open large losses in Victoria"
11. "What is the average large loss by product and state?"
12. "How many large losses are still open after 2 years?"

### IFRS 17 & Portfolio
13. "Which cohorts have loss ratios above 80%?"
14. "Show total gross written premium by product"
15. "What is the average claim count per cohort?"
16. "List cohorts flagged as 'ABOVE_100' for immediate action"

### Cross-Analysis
17. "Compare loss ratios between states"
18. "Show me the correlation between policy count and loss ratio"
19. "What percentage of claims become large losses?"
20. "Which perils drive the highest incurred amounts?"

## SQL Expressions

For advanced queries, use these SQL expressions:

**Filter to recent accident years:**
```sql
WHERE accident_year >= 2022
```

**Calculate paid-to-incurred ratio:**
```sql
SELECT cumulative_paid / NULLIF(cumulative_incurred, 0) as paid_pct
```

**Find deteriorating reserves:**
```sql
WHERE rag_status IN ('AMBER', 'RED') AND deteriorating_flag = true
```

**Aggregate by product:**
```sql
GROUP BY product
ORDER BY SUM(cumulative_incurred) DESC
```

## Data Refresh

Data is refreshed via DLT pipeline. Use time travel for historical comparisons:

```sql
SELECT * FROM actuary_corpfin.gold.development_triangles
TIMESTAMP AS OF '2025-12-31'
```

## Glossary

- **Triangle**: Matrix showing claims development over time
- **Incurred**: Total estimated cost including reserves
- **Paid**: Actual payments made to date
- **IBNR**: Incurred But Not Reported reserves
- **RAG**: Red/Amber/Green status indicators
- **Cohort**: Group of policies with same inception period
- **Development Period**: Time elapsed since accident date
- **Ultimate**: Final projected claim amount

## Tips

- Use product names: 'home', 'motor', 'CTP'
- States: NSW, VIC, QLD, WA, SA
- Development periods: 12, 24, 36, 48, 60 months
- Always specify time ranges for better performance
- Use "LIMIT" for large result sets
