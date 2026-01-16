{{ config(materialized='table') }}

SELECT
    l_returnflag,
    l_linestatus,
    DATE_TRUNC('month', l_shipdate) AS ship_month,
    SUM(l_quantity) AS sum_qty,
    SUM(l_extendedprice) AS sum_base_price,
    SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    AVG(l_quantity) AS avg_qty,
    AVG(l_extendedprice) AS avg_price,
    AVG(l_discount) AS avg_disc,
    COUNT(*) AS count_order,
    SUM(SUM(l_extendedprice)) OVER (PARTITION BY l_returnflag ORDER BY DATE_TRUNC('month', l_shipdate)) AS running_total
FROM
    SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.LINEITEM
WHERE
    DATE(l_shipdate) >= '1994-01-01'
    AND DATE(l_shipdate) < '1998-12-01'
GROUP BY
    l_returnflag,
    l_linestatus,
    DATE_TRUNC('month', l_shipdate)
ORDER BY
    l_returnflag,
    l_linestatus,
    ship_month
