{{ config(materialized='table') }}

SELECT
    n.n_name AS nation,
    YEAR(o.o_orderdate) AS o_year,
    SUM(l.l_extendedprice * (1 - l.l_discount) - ps.ps_supplycost * l.l_quantity) AS sum_profit,
    COUNT(*) AS order_count
FROM
    SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.PART p
    INNER JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.PARTSUPP ps
        ON p.p_partkey = ps.ps_partkey
    INNER JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.LINEITEM l
        ON ps.ps_suppkey = l.l_suppkey
        AND ps.ps_partkey = l.l_partkey
    INNER JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS o
        ON l.l_orderkey = o.o_orderkey
    INNER JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.SUPPLIER s
        ON l.l_suppkey = s.s_suppkey
    INNER JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.NATION n
        ON s.s_nationkey = n.n_nationkey
WHERE
    p.p_name LIKE '%green%'
    AND YEAR(o.o_orderdate) BETWEEN 1993 AND 1997
GROUP BY
    n.n_name,
    YEAR(o.o_orderdate)
ORDER BY
    nation,
    o_year DESC
