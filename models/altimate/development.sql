{{ config(materialized='table') }}

SELECT
    c_custkey,
    c_name,
    c_mktsegment,
    c_acctbal,
    (SELECT COUNT(*)
     FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS o
     WHERE o.o_custkey = c.c_custkey
       AND o.o_orderdate >= '1995-01-01'
       AND o.o_orderdate < '1996-01-01') as order_count_1995,
    (SELECT SUM(o.o_totalprice)
     FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS o
     WHERE o.o_custkey = c.c_custkey
       AND o.o_orderdate >= '1995-01-01'
       AND o.o_orderdate < '1996-01-01') as total_spent_1995,
    (SELECT AVG(o.o_totalprice)
     FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS o
     WHERE o.o_custkey = c.c_custkey
       AND o.o_orderdate >= '1995-01-01'
       AND o.o_orderdate < '1996-01-01') as avg_order_value
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER c
WHERE c.c_mktsegment IN ('BUILDING', 'AUTOMOBILE', 'MACHINERY')
ORDER BY c_custkey
LIMIT 10000
