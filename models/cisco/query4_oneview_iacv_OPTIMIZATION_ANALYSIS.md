# Query 4 OneView IACV - Optimization Analysis

## Executive Summary
This document outlines optimization opportunities for the OneView IACV DBT model. The query has significant complexity with multiple CTEs, conditional aggregations, and cross joins.

## Static Analysis Findings

### 1. FUNCTION_IN_FILTER Warnings (8 occurrences)
**Issue**: CASE expressions in WHERE clauses may prevent partition pruning
- `CASE WHEN SAV_SALES_LEVEL_1 = 'EMEA_GEO' THEN 'EMEA' ELSE SAV_SALES_LEVEL_1 END`
- `CASE PMG_IN_SCOPE WHEN 'SW-N' THEN 'N' WHEN 'SVS' THEN 'Y' WHEN 'SW-Y' THEN 'Y' ELSE PMG_IN_SCOPE END`

**Recommendation**: 
- Cannot be eliminated as they're business logic requirements
- Could create a materialized view with these transformations pre-computed
- Verify if these expressions match clustering keys (if so, pruning still works)

### 2. CARTESIAN_PRODUCT (CROSS JOIN)
**Issue**: CROSS JOIN with subquery `t2` that returns a single row with aggregate measures
**Impact**: Since `t2` returns a single row, the Cartesian product is intentional for broadcasting these measures to all rows
**Recommendation**: This is acceptable but document the reason for the CROSS JOIN

## DBT Best Practices & Optimization Opportunities

### 1. **Extract Common Expressions as CTEs**
**Current State**: 
- EMEA_GEO transformation appears 8+ times throughout the query
- PMG_IN_SCOPE transformation appears 2+ times
- Hardcoded values (`20263`, `121`, `'wwang8'`) appear multiple times

**Recommendation**:
```sql
-- Create base CTE with transformations
WITH base_data AS (
    SELECT
        *,
        CASE WHEN SAV_SALES_LEVEL_1 = 'EMEA_GEO' THEN 'EMEA' 
             ELSE SAV_SALES_LEVEL_1 END AS normalized_sales_level_1,
        CASE PMG_IN_SCOPE 
             WHEN 'SW-N' THEN 'N' 
             WHEN 'SVS' THEN 'Y' 
             WHEN 'SW-Y' THEN 'Y' 
             ELSE PMG_IN_SCOPE 
        END AS normalized_pmg_scope
    FROM {{ source('cx_db_cx_grit_br', 'oneview_iacv_tbl') }}
)
```

**Benefits**:
- Compute once, use many times
- Easier to maintain
- Clearer intent
- Potential for Snowflake to optimize better

### 2. **Parameterize Hardcoded Values**
**Current State**: Hardcoded values scattered throughout
- Quarter: `20263`
- Quarter Key: `121`
- User: `'wwang8'`
- Portfolio values: `'CAI'`, `'APJC'`

**Recommendation**:
```sql
{% set target_quarter = var('target_quarter', 20263) %}
{% set target_quarter_key = var('target_quarter_key', 121) %}
{% set target_user = var('target_user', 'wwang8') %}
{% set target_portfolio = var('target_portfolio', 'CAI') %}
{% set target_region = var('target_region', 'APJC') %}
```

**Benefits**:
- Reusable for different quarters/users
- Easier testing
- Better CI/CD integration

### 3. **Remove Redundant Conditions**
**Current State**: `'CurrentWeek' = 'CurrentWeek'` appears in multiple CASE statements

**Recommendation**: Remove entirely as it's always TRUE

**Benefits**:
- Cleaner code
- Slight performance improvement

### 4. **Simplify Complex WHERE Clauses**
**Current State**: Deeply nested AND/OR conditions with multiple parentheses

**Recommendation**: Break into logical CTEs with clear names
```sql
WITH filtered_base AS (
    SELECT * 
    FROM base_data
    WHERE normalized_pmg_scope = 'Y'
      AND SOURCE NOT IN ('OneView_iacv_metrics', 'OneView_snapshot_atr', 'sfdc_pipeline')
      AND normalized_sales_level_1 IN ('APJC', 'Americas', 'EMEA')
      AND normalized_sales_level_1 = '{{ target_region }}'
),
user_filtered AS (
    SELECT *
    FROM filtered_base
    WHERE '{{ target_user }}' IN (RM_CEC, RM2_CEC, RM3_CEC, RS_CEC, RS2_CEC)
)
```

### 5. **Consolidate Duplicate Subqueries**
**Current State**: Subqueries `t0` and `t1` are nearly identical

**Recommendation**: Create a single CTE and reference it twice
```sql
WITH sales_level_filter AS (
    SELECT
        SAV_SALES_LEVEL_2 AS sales_level_2_key,
        1 AS is_filtered
    FROM base_data
    WHERE normalized_sales_level_1 IN ('APJC', 'Americas', 'EMEA')
      AND SAV_SALES_LEVEL_2 IN ('APJ_SP', 'GREATER_CHINA', 'JAPAN__', 'ROK_AREA')
    GROUP BY 1
)
```

### 6. **Organize Conditional Aggregations by Business Logic**
**Current State**: 14 SUM(CASE...) expressions in SELECT clause

**Recommendation**: Group by business purpose with comments
```sql
SELECT
    -- Sales Level Calculation
    dimension_calculation,
    
    -- Renewed Plan Metrics (Quarter {{ target_quarter }})
    SUM(CASE WHEN GOALING_PERIOD_QUARTER = {{ target_quarter }} 
             THEN STANDARD_RR_PLAN_DENOMINATOR ELSE 0 END) AS renewed_plan_denominator,
    SUM(CASE WHEN GOALING_PERIOD_QUARTER = {{ target_quarter }} 
             THEN STANDARD_RR_PLAN_NUMERATOR ELSE 0 END) AS renewed_plan_numerator,
    
    -- IQRR Metrics
    -- ... grouped logically
```

### 7. **Add Incremental Loading Strategy**
**Current State**: Full refresh on every run

**Recommendation**: If data supports it, add incremental strategy
```sql
{{ config(
    materialized='incremental',
    unique_key='<appropriate_key>',
    incremental_strategy='merge',
    query_tag='cisco_demo'
) }}

{% if is_incremental() %}
WHERE updated_timestamp > (SELECT MAX(updated_timestamp) FROM {{ this }})
{% endif %}
```

### 8. **Improve Column Naming**
**Current State**: Tableau-generated names like `TEMP(Calculation_2725240741616009228)(2622581296)(0)`

**Recommendation**: Use business-meaningful aliases
```sql
SUM(...) AS iqrr_numerator_selected_qtr_cw
SUM(...) AS iqrr_denominator_selected_qtr_cw
SUM(...) AS quarterline_upside_selected_qtr_cw
```

## Performance Impact Estimation

| Optimization | Estimated Impact | Effort |
|-------------|------------------|--------|
| Extract common CASE expressions | Medium (5-10% improvement) | Low |
| Remove `'CurrentWeek' = 'CurrentWeek'` | Low (1-2% improvement) | Low |
| Consolidate duplicate subqueries | Low-Medium (3-5% improvement) | Low |
| Parameterize hardcoded values | None (maintainability) | Low |
| Better column names | None (readability) | Low |
| Incremental loading | High (50%+ for subsequent runs) | Medium |
| WHERE clause simplification | Low (2-3% improvement) | Medium |

## Implementation Priority

1. **High Priority** (Quick wins)
   - Extract common CASE expressions into base CTE
   - Remove redundant `'CurrentWeek' = 'CurrentWeek'` conditions
   - Improve column naming
   - Parameterize hardcoded values

2. **Medium Priority**
   - Consolidate duplicate subqueries
   - Organize WHERE clauses into logical CTEs
   - Add comments explaining business logic

3. **Low Priority** (Requires business validation)
   - Implement incremental loading strategy
   - Consider materializing intermediate transformations

## Testing Recommendations

1. **Data Quality Tests**
   - Row count comparison (original vs optimized)
   - Sum of all numeric columns
   - HAVING clause results (should still filter correctly)

2. **Performance Tests**
   - Query execution time comparison
   - Bytes scanned comparison
   - Credits consumed comparison

3. **Logic Validation**
   - Sample 1000 random rows, compare all columns
   - Test edge cases (NULL handling, division by zero)

## Next Steps

1. Review and approve optimization priorities
2. Implement high-priority optimizations
3. Test in development environment
4. Compare performance metrics
5. Deploy to production with monitoring
