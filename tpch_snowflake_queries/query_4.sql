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
    AND CAST(l_orderkey AS VARCHAR) = CAST(o_orderkey AS VARCHAR)
    AND c_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name IN ('ASIA', 'EUROPE')
    AND o_orderdate >= '1994-01-01'
    AND o_orderdate < '1995-01-01'
    AND o_totalprice > (
        SELECT AVG(o2.o_totalprice)
        FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS o2
        WHERE YEAR(o2.o_orderdate) = 1994
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