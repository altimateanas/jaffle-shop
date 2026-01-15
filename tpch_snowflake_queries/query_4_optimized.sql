-- Optimized Query: Removed functions from filter conditions
-- Original query: tpch_snowflake_queries/query_4.sql
-- Optimization opportunities identified and fixed:
-- 1. Removed CAST functions from JOIN condition (direct numeric join)
-- 2. Replaced YEAR() function with date range filter for better partition pruning

SELECT DISTINCT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    SUM(l_quantity) AS total_quantity,
    n_name AS nation,
    r_name AS region
FROM
    SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER,
    SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS,
    SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.LINEITEM,
    SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.NATION,
    SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.REGION
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND c_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name IN ('ASIA', 'EUROPE')
    AND o_orderdate >= '1994-01-01'
    AND o_orderdate < '1995-01-01'
    AND o_totalprice > (
        SELECT AVG(o2.o_totalprice)
        FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS o2
        WHERE o2.o_orderdate >= '1994-01-01'
        AND o2.o_orderdate < '1995-01-01'
    )
GROUP BY
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    n_name,
    r_name
ORDER BY
    o_totalprice DESC,
    o_orderdate
LIMIT 100;
