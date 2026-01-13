SELECT
    n_name AS nation,
    YEAR(o_orderdate) AS o_year,
    SUM(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) AS sum_profit,
    COUNT(*) AS order_count
FROM
    SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.PART,
    SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.SUPPLIER,
    SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.LINEITEM,
    SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.PARTSUPP,
    SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS,
    SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.NATION
WHERE
    s_suppkey = l_suppkey
    AND ps_suppkey = l_suppkey
    AND ps_partkey = l_partkey
    AND p_partkey = l_partkey
    AND o_orderkey = l_orderkey
    AND s_nationkey = n_nationkey
    AND p_name LIKE '%green%'
    AND YEAR(o_orderdate) BETWEEN 1993 AND 1997
GROUP BY
    n_name,
    YEAR(o_orderdate)
ORDER BY
    nation,
    o_year DESC;