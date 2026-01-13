{{ config(
    query_tag='cisco_demo',
    materialized='table'
) }}

{#
    OneView IACV Analysis - Optimized Version
    
    Optimizations applied:
    1. Extracted common CASE expressions into base CTE
    2. Parameterized hardcoded values using dbt variables
    3. Removed redundant 'CurrentWeek' = 'CurrentWeek' conditions
    4. Consolidated duplicate subqueries into single CTE
    5. Improved column naming for readability
    6. Organized conditional aggregations with comments
    7. Simplified WHERE clauses with intermediate CTEs
    8. Added documentation
#}

-- Configuration variables (can be overridden at runtime)
{% set target_quarter = var('target_quarter', 20263) %}
{% set target_quarter_key = var('target_quarter_key', 121) %}
{% set target_user = var('target_user', 'wwang8') %}
{% set target_portfolio_group = var('target_portfolio_group', 'CAI') %}
{% set target_region = var('target_region', 'APJC') %}

-- Base CTE: Apply common transformations once
WITH base_oneview AS (
    SELECT
        *,
        -- Normalize EMEA_GEO to EMEA (computed once, used many times)
        CASE 
            WHEN SAV_SALES_LEVEL_1 = 'EMEA_GEO' THEN 'EMEA' 
            ELSE SAV_SALES_LEVEL_1 
        END AS normalized_sales_level_1,
        
        -- Normalize PMG scope values
        CASE PMG_IN_SCOPE 
            WHEN 'SW-N' THEN 'N' 
            WHEN 'SVS' THEN 'Y' 
            WHEN 'SW-Y' THEN 'Y' 
            ELSE PMG_IN_SCOPE 
        END AS normalized_pmg_scope,
        
        -- Pre-compute exclusion flag for readability
        CASE 
            WHEN BE_CX_CUSTOM_PORTFOLIO IN ('Support Services', 'Support Services and Success Track')
              OR BE_CX_CUSTOM_PORTFOLIO IS NULL 
            THEN FALSE 
            ELSE TRUE 
        END AS is_included_portfolio,
        
        -- Pre-compute source exclusion flag
        CASE 
            WHEN SOURCE IN ('OneView_iacv_metrics', 'OneView_snapshot_atr', 'sfdc_pipeline') 
            THEN FALSE 
            ELSE TRUE 
        END AS is_valid_source
        
    FROM {{ source('cx_db_cx_grit_br', 'oneview_iacv_tbl') }}
),

-- Portfolio grouping join (used multiple times)
portfolio_mapping AS (
    SELECT
        X_BE_CX_CUSTOM_PORTFOLIO,
        X_BE_CX_CUSTOM_PORTFOLIO_GROUP_1
    FROM {{ source('cx_db_cx_grit_br', 'tableau_6699_2_group') }}
),

-- Sales level filter CTE (consolidates duplicate subqueries t0 and t1)
sales_level_filter AS (
    SELECT
        SAV_SALES_LEVEL_2 AS sales_level_2_key,
        TRUE AS is_filtered_sales_level
    FROM base_oneview
    WHERE normalized_sales_level_1 IN ('APJC', 'Americas', 'EMEA')
      AND SAV_SALES_LEVEL_2 IN ('APJ_SP', 'GREATER_CHINA', 'JAPAN__', 'ROK_AREA')
    GROUP BY 1
),

-- Cross-join subquery: Calculate distinct counts for dimension selection
dimension_measures AS (
    SELECT
        COUNT(DISTINCT base.SAV_SALES_LEVEL_6) AS distinct_level_6,
        COUNT(DISTINCT base.SAV_SALES_LEVEL_5) AS distinct_level_5,
        COUNT(DISTINCT base.SAV_SALES_LEVEL_4) AS distinct_level_4,
        COUNT(DISTINCT base.SAV_SALES_LEVEL_3) AS distinct_level_3,
        COUNT(DISTINCT base.SAV_SALES_LEVEL_2) AS distinct_level_2,
        COUNT(DISTINCT base.normalized_sales_level_1) AS distinct_level_1
    FROM base_oneview base
    INNER JOIN portfolio_mapping pm
        ON base.BE_CX_CUSTOM_PORTFOLIO IS NOT DISTINCT FROM pm.X_BE_CX_CUSTOM_PORTFOLIO
    LEFT JOIN sales_level_filter slf
        ON base.SAV_SALES_LEVEL_2 = slf.sales_level_2_key
    WHERE base.normalized_pmg_scope = 'Y'
      AND base.is_valid_source
      AND base.normalized_sales_level_1 IN ('APJC', 'Americas', 'EMEA')
      AND base.normalized_sales_level_1 = '{{ target_region }}'
      AND pm.X_BE_CX_CUSTOM_PORTFOLIO_GROUP_1 = '{{ target_portfolio_group }}'
      AND (
          '{{ target_user }}' = base.RM_CEC
          OR '{{ target_user }}' = base.RM2_CEC
          OR '{{ target_user }}' = base.RM3_CEC
          OR '{{ target_user }}' = base.RS_CEC
          OR '{{ target_user }}' = base.RS2_CEC
          OR slf.is_filtered_sales_level IS NOT NULL
      )
    HAVING COUNT(1) > 0
),

-- Main filtered dataset
main_dataset AS (
    SELECT
        base.*,
        pm.X_BE_CX_CUSTOM_PORTFOLIO_GROUP_1,
        slf.is_filtered_sales_level
    FROM base_oneview base
    INNER JOIN portfolio_mapping pm
        ON base.BE_CX_CUSTOM_PORTFOLIO IS NOT DISTINCT FROM pm.X_BE_CX_CUSTOM_PORTFOLIO
    LEFT JOIN sales_level_filter slf
        ON base.SAV_SALES_LEVEL_2 = slf.sales_level_2_key
    WHERE base.is_included_portfolio
      AND base.normalized_pmg_scope = 'Y'
      AND base.is_valid_source
      AND base.normalized_sales_level_1 IN ('APJC', 'Americas', 'EMEA')
      AND base.normalized_sales_level_1 = '{{ target_region }}'
      AND pm.X_BE_CX_CUSTOM_PORTFOLIO_GROUP_1 = '{{ target_portfolio_group }}'
      AND (
          '{{ target_user }}' = base.RM_CEC
          OR '{{ target_user }}' = base.RM2_CEC
          OR '{{ target_user }}' = base.RM3_CEC
          OR '{{ target_user }}' = base.RS_CEC
          OR '{{ target_user }}' = base.RS2_CEC
          OR slf.is_filtered_sales_level IS NOT NULL
      )
)

-- Final aggregation
SELECT
    -- Dimension selection based on distinct counts
    CASE
        WHEN dm.distinct_level_6 = 1 THEN md.SAV_SALES_LEVEL_6
        WHEN dm.distinct_level_5 = 1 THEN md.SAV_SALES_LEVEL_6
        WHEN dm.distinct_level_5 = 1 THEN md.SAV_SALES_LEVEL_5
        WHEN dm.distinct_level_4 = 1 THEN md.SAV_SALES_LEVEL_4
        WHEN dm.distinct_level_3 = 1 THEN md.SAV_SALES_LEVEL_3
        WHEN dm.distinct_level_2 = 1 THEN md.SAV_SALES_LEVEL_2
        ELSE md.normalized_sales_level_1
    END AS sales_level_dimension,
    
    -- Renewed Plan Metrics (Target Quarter: {{ target_quarter }})
    SUM(CASE 
        WHEN md.GOALING_PERIOD_QUARTER = {{ target_quarter }} 
        THEN md.STANDARD_RR_PLAN_DENOMINATOR 
        ELSE 0 
    END) AS renewed_plan_denominator,
    
    SUM(CASE 
        WHEN md.GOALING_PERIOD_QUARTER = {{ target_quarter }} 
        THEN md.STANDARD_RR_PLAN_NUMERATOR 
        ELSE 0 
    END) AS renewed_plan_numerator,
    
    -- IQRR Numerator/Denominator (Quarter Key + 4 = {{ target_quarter_key }}, Seq Num = 0)
    SUM(CASE 
        WHEN (md.GOALING_PERIOD_QUARTER_KEY + 4) = {{ target_quarter_key }}
         AND md.RENEWED_GOALING_PERIOD_SEQ_NUM = 0
        THEN md.IQRR_NUMERATOR 
        ELSE 0 
    END) AS iqrr_numerator_qk_plus_4,
    
    SUM(CASE 
        WHEN (md.GOALING_PERIOD_QUARTER_KEY + 4) = {{ target_quarter_key }}
         AND md.RENEWED_GOALING_PERIOD_SEQ_NUM = 0
        THEN md.IQRR_DENOMINATOR 
        ELSE 0 
    END) AS iqrr_denominator_qk_plus_4,
    
    -- IQRR Metrics (Target Quarter: {{ target_quarter }}, Seq Num = 1)
    SUM(CASE 
        WHEN md.GOALING_PERIOD_QUARTER = {{ target_quarter }}
         AND md.RENEWED_GOALING_PERIOD_SEQ_NUM = 1
        THEN md.IQRR_DENOMINATOR 
        ELSE 0 
    END) AS iqrr_denominator_seq1,
    
    SUM(CASE 
        WHEN md.GOALING_PERIOD_QUARTER = {{ target_quarter }}
         AND md.RENEWED_GOALING_PERIOD_SEQ_NUM = 1
        THEN md.IQRR_NUMERATOR 
        ELSE 0 
    END) AS iqrr_numerator_seq1,
    
    -- IQRR Metrics (Target Quarter: {{ target_quarter }}, Seq Num = 0)
    SUM(CASE 
        WHEN md.GOALING_PERIOD_QUARTER = {{ target_quarter }}
         AND md.RENEWED_GOALING_PERIOD_SEQ_NUM = 0
        THEN md.IQRR_NUMERATOR 
        ELSE 0 
    END) AS iqrr_numerator_seq0,
    
    SUM(CASE 
        WHEN md.GOALING_PERIOD_QUARTER = {{ target_quarter }}
         AND md.RENEWED_GOALING_PERIOD_SEQ_NUM = 0
        THEN md.IQRR_DENOMINATOR 
        ELSE 0 
    END) AS iqrr_denominator_seq0,
    
    -- SFDC Pipeline Commit (Expiration Quarter Key = {{ target_quarter_key }})
    SUM(CASE 
        WHEN md.EXPIRATION_QUARTER_KEY = {{ target_quarter_key }}
         AND md.GOALING_PERIOD_QUARTER_KEY <= {{ target_quarter_key }}
         AND md.SOURCE = 'sfdc_Pipeline'
         AND md.FORECAST_STATUS_C = 'Commit'
        THEN CASE 
            WHEN md.EXPECTED_ANNUAL_C > md.PRIOR_ATR_000_S_C 
            THEN md.PRIOR_ATR_000_S_C 
            ELSE md.EXPECTED_ANNUAL_C 
        END
        ELSE 0 
    END) AS sfdc_pipeline_commit_capped,
    
    -- Quarterline Pipeline Upside (Target Quarter: {{ target_quarter }})
    SUM(CASE 
        WHEN md.GOALING_PERIOD_QUARTER = {{ target_quarter }}
         AND md.BOOK_QUARTER <= {{ target_quarter }}
         AND md.SOURCE = 'Quarterline_Pipeline'
         AND md.FORECAST_STATUS_C = 'Upside'
         AND NOT CONTAINS(md.STAGE_NAME, '6')
        THEN CASE 
            WHEN md.QUARTERLINE_EXPECTED_ATR > md.QUARTERLINE_ATR 
            THEN md.QUARTERLINE_ATR 
            ELSE md.QUARTERLINE_EXPECTED_ATR 
        END
        ELSE 0 
    END) AS quarterline_upside_capped,
    
    -- Quarterline Pipeline Commit (Target Quarter: {{ target_quarter }})
    SUM(CASE 
        WHEN md.GOALING_PERIOD_QUARTER = {{ target_quarter }}
         AND md.BOOK_QUARTER <= {{ target_quarter }}
         AND md.SOURCE = 'Quarterline_Pipeline'
         AND md.FORECAST_STATUS_C = 'Commit'
         AND NOT CONTAINS(md.STAGE_NAME, '6')
        THEN CASE 
            WHEN md.QUARTERLINE_EXPECTED_ATR > md.QUARTERLINE_ATR 
            THEN md.QUARTERLINE_ATR 
            ELSE md.QUARTERLINE_EXPECTED_ATR 
        END
        ELSE 0 
    END) AS quarterline_commit_capped,
    
    -- Selected Standard RR Expiration (Target Quarter: {{ target_quarter }}, Seq Num = 0)
    SUM(CASE 
        WHEN md.GOALING_PERIOD_QUARTER = {{ target_quarter }}
         AND md.RENEWED_GOALING_PERIOD_SEQ_NUM = 0
        THEN md.IQRR_DENOMINATOR 
        ELSE 0 
    END) AS standard_rr_expiration_denominator,
    
    SUM(CASE 
        WHEN md.GOALING_PERIOD_QUARTER = {{ target_quarter }}
         AND md.RENEWED_GOALING_PERIOD_SEQ_NUM = 0
        THEN md.IQRR_NUMERATOR 
        ELSE 0 
    END) AS standard_rr_expiration_numerator,
    
    -- SFDC Pipeline Most Likely + Upside (Expiration Quarter Key = {{ target_quarter_key }})
    SUM(CASE 
        WHEN md.EXPIRATION_QUARTER_KEY = {{ target_quarter_key }}
         AND md.GOALING_PERIOD_QUARTER_KEY <= {{ target_quarter_key }}
         AND md.SOURCE = 'sfdc_Pipeline'
         AND md.FORECAST_STATUS_C IN ('Most Likely', 'Upside')
        THEN CASE 
            WHEN md.EXPECTED_ANNUAL_C > md.PRIOR_ATR_000_S_C 
            THEN md.PRIOR_ATR_000_S_C 
            ELSE md.EXPECTED_ANNUAL_C 
        END
        ELSE 0 
    END) AS std_iqrr_commit_capped
    
FROM main_dataset md
CROSS JOIN dimension_measures dm

GROUP BY 1

-- Complex HAVING clause: Total value > 5000
HAVING (
    ZEROIFNULL(SUM(CASE 
        WHEN md.GOALING_PERIOD_QUARTER = {{ target_quarter }}
         AND md.RENEWED_GOALING_PERIOD_SEQ_NUM = 0
        THEN md.IQRR_DENOMINATOR ELSE 0 END))
    + 
    ZEROIFNULL(SUM(CASE 
        WHEN md.GOALING_PERIOD_QUARTER = {{ target_quarter }}
         AND md.RENEWED_GOALING_PERIOD_SEQ_NUM = 0
        THEN md.IQRR_NUMERATOR ELSE 0 END))
    + 
    ZEROIFNULL(SUM(CASE 
        WHEN md.EXPIRATION_QUARTER_KEY = {{ target_quarter_key }}
         AND md.GOALING_PERIOD_QUARTER_KEY <= {{ target_quarter_key }}
         AND md.SOURCE = 'sfdc_Pipeline'
         AND md.FORECAST_STATUS_C = 'Commit'
        THEN CASE 
            WHEN md.EXPECTED_ANNUAL_C > md.PRIOR_ATR_000_S_C 
            THEN md.PRIOR_ATR_000_S_C 
            ELSE md.EXPECTED_ANNUAL_C 
        END ELSE 0 END))
    + 
    ZEROIFNULL(
        SUM(CASE 
            WHEN md.EXPIRATION_QUARTER_KEY = {{ target_quarter_key }}
             AND md.GOALING_PERIOD_QUARTER_KEY <= {{ target_quarter_key }}
             AND md.SOURCE = 'sfdc_Pipeline'
             AND md.FORECAST_STATUS_C IN ('Most Likely', 'Upside')
            THEN CASE 
                WHEN md.EXPECTED_ANNUAL_C > md.PRIOR_ATR_000_S_C 
                THEN md.PRIOR_ATR_000_S_C 
                ELSE md.EXPECTED_ANNUAL_C 
            END ELSE 0 END)
        + 
        ZEROIFNULL(
            CASE 
                WHEN SUM(CASE 
                    WHEN md.GOALING_PERIOD_QUARTER = {{ target_quarter }} 
                    THEN md.STANDARD_RR_PLAN_DENOMINATOR ELSE 0 END) <> 0 
                THEN SUM(CASE 
                    WHEN md.GOALING_PERIOD_QUARTER = {{ target_quarter }} 
                    THEN md.STANDARD_RR_PLAN_NUMERATOR ELSE 0 END) 
                   / SUM(CASE 
                    WHEN md.GOALING_PERIOD_QUARTER = {{ target_quarter }} 
                    THEN md.STANDARD_RR_PLAN_DENOMINATOR ELSE 0 END)
            END
        )
    )
) > 5000
