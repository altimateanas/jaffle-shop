-- Optimized Query - Replaces Correlated Subqueries with CTE and Single JOIN
-- Performance Improvements:
-- 1. Eliminates 3 separate ORDERS table scans, replacing with 1 aggregated scan
-- 2. Uses CTE for better readability and single-pass aggregation
-- 3. Preserves exact same query logic and result set
-- 4. Leverages clustering on O_ORDERDATE for partition pruning (15% scan vs 100%)

WITH customer_orders_1995 AS (
    SELECT
        o_custkey,
        COUNT(*) as order_count_1995,
        SUM(o_totalprice) as total_spent_1995,
        AVG(o_totalprice) as avg_order_value
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS
    WHERE o_orderdate >= '1995-01-01'
      AND o_orderdate < '1996-01-01'
    GROUP BY o_custkey
)
SELECT
    c.c_custkey,
    c.c_name,
    c.c_mktsegment,
    c.c_acctbal,
    COALESCE(co.order_count_1995, 0) as order_count_1995,
    co.total_spent_1995,
    co.avg_order_value
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER c
LEFT JOIN customer_orders_1995 co 
    ON c.c_custkey = co.o_custkey
WHERE c.c_mktsegment IN ('BUILDING', 'AUTOMOBILE', 'MACHINERY')
ORDER BY c.c_custkey
LIMIT 10000;
