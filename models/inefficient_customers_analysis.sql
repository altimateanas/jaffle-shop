{{
    config(
        materialized='table'
    )
}}

/*
    ⚠️ EXTREMELY INEFFICIENT MODEL - FOR OPTIMIZATION DEMONSTRATION ONLY ⚠️
    
    This model showcases 20+ anti-patterns that will result in:
    - High query costs
    - Slow execution times
    - Excessive resource consumption
    - Memory spillage
    
    ANTI-PATTERNS INCLUDED:
    1.  SELECT * everywhere
    2.  Multiple CROSS JOINs
    3.  Nested DISTINCT operations
    4.  Correlated subqueries (N+1 pattern)
    5.  Same table scanned 5+ times
    6.  Non-sargable WHERE clauses
    7.  Excessive window functions
    8.  Heavy string manipulations
    9.  UNION ALL instead of single query
    10. ORDER BY in subqueries
    11. Unused CTEs (dead code)
    12. Deep query nesting
    13. Cartesian products
    14. Redundant aggregations
    15. Multiple hash operations
    16. Regular expressions
    17. Type conversions
    18. Date calculations in filters
    19. Self-joins
    20. Final ORDER BY on table
*/

-- ============================================================================
-- LAYER 1: Base CTEs with SELECT * anti-pattern
-- ============================================================================

with all_customers as (
    select * from {{ ref('stg_customers') }}
),

all_orders as (
    select * from {{ ref('stg_orders') }}
),

all_payments as (
    select * from {{ ref('stg_payments') }}
),

-- ============================================================================
-- LAYER 2: Redundant table scans (same source read multiple times)
-- ============================================================================

customers_scan_2 as (
    select * from {{ ref('stg_customers') }}
),

customers_scan_3 as (
    select * from {{ ref('stg_customers') }}
),

orders_scan_2 as (
    select * from {{ ref('stg_orders') }}
),

orders_scan_3 as (
    select * from {{ ref('stg_orders') }}
),

payments_scan_2 as (
    select * from {{ ref('stg_payments') }}
),

-- ============================================================================
-- LAYER 3: Unnecessary ORDER BY in CTEs (sorting that gets discarded)
-- ============================================================================

sorted_customers as (
    select 
        customer_id,
        first_name,
        last_name
    from all_customers
    order by customer_id desc, first_name asc, last_name desc
),

sorted_orders as (
    select 
        order_id,
        customer_id,
        order_date,
        status
    from all_orders
    order by order_date desc, order_id desc, customer_id asc, status
),

sorted_payments as (
    select 
        payment_id,
        order_id,
        payment_method,
        amount
    from all_payments
    order by amount desc, payment_id asc, order_id desc
),

-- ============================================================================
-- LAYER 4: CROSS JOINs creating massive intermediate results
-- ============================================================================

-- Cross join customers with orders (every customer x every order)
customer_order_cross as (
    select 
        c.customer_id as cust_id,
        c.first_name,
        c.last_name,
        o.order_id,
        o.order_date,
        o.status
    from sorted_customers c
    cross join sorted_orders o
),

-- Cross join customers with payments (every customer x every payment)
customer_payment_cross as (
    select 
        c.customer_id as cust_id,
        c.first_name,
        c.last_name,
        p.payment_id,
        p.payment_method,
        p.amount
    from sorted_customers c
    cross join sorted_payments p
),

-- Triple cross join (customers x orders x payments) - MASSIVE
triple_cross as (
    select 
        c.customer_id as cust_id,
        c.first_name,
        c.last_name,
        o.order_id,
        o.order_date,
        p.payment_id,
        p.payment_method,
        p.amount
    from all_customers c
    cross join all_orders o
    cross join all_payments p
),

-- ============================================================================
-- LAYER 5: DISTINCT on massive cross-joined datasets
-- ============================================================================

distinct_customer_orders as (
    select distinct
        cust_id,
        first_name,
        last_name,
        order_id,
        status
    from customer_order_cross
),

distinct_customer_payments as (
    select distinct
        cust_id,
        first_name,
        last_name,
        payment_method
    from customer_payment_cross
),

distinct_triple as (
    select distinct
        cust_id,
        order_id,
        payment_method
    from triple_cross
),

-- ============================================================================
-- LAYER 6: Non-sargable WHERE clauses (functions on columns)
-- ============================================================================

filtered_by_functions as (
    select 
        order_id,
        customer_id,
        order_date,
        status
    from all_orders
    where upper(status) = 'COMPLETED'
       or lower(status) = 'shipped'
       or trim(status) like '%placed%'
       or length(status) > 3
       or left(status, 1) = 'c'
       or right(status, 2) = 'ed'
       or position('e' in status) > 0
       or regexp_like(status, '.*[a-z].*')
),

-- ============================================================================
-- LAYER 7: Correlated subqueries (N+1 query pattern)
-- ============================================================================

customer_with_correlated_stats as (
    select 
        c.customer_id,
        c.first_name,
        c.last_name,
        -- Correlated subquery 1: count orders
        (
            select count(*) 
            from all_orders o 
            where o.customer_id = c.customer_id
        ) as order_count,
        -- Correlated subquery 2: max order date
        (
            select max(order_date) 
            from all_orders o 
            where o.customer_id = c.customer_id
        ) as latest_order_date,
        -- Correlated subquery 3: min order date
        (
            select min(order_date) 
            from all_orders o 
            where o.customer_id = c.customer_id
        ) as first_order_date,
        -- Correlated subquery 4: count distinct statuses
        (
            select count(distinct status) 
            from all_orders o 
            where o.customer_id = c.customer_id
        ) as distinct_statuses,
        -- Correlated subquery 5: total payment amount (nested join in subquery)
        (
            select coalesce(sum(p.amount), 0)
            from all_payments p
            inner join all_orders o on p.order_id = o.order_id
            where o.customer_id = c.customer_id
        ) as total_spent,
        -- Correlated subquery 6: count payments
        (
            select count(*)
            from all_payments p
            inner join all_orders o on p.order_id = o.order_id
            where o.customer_id = c.customer_id
        ) as payment_count,
        -- Correlated subquery 7: avg payment
        (
            select avg(p.amount)
            from all_payments p
            inner join all_orders o on p.order_id = o.order_id
            where o.customer_id = c.customer_id
        ) as avg_payment
    from all_customers c
),

-- ============================================================================
-- LAYER 8: Heavy string manipulations
-- ============================================================================

string_heavy_customers as (
    select
        customer_id,
        first_name,
        last_name,
        -- Concatenations
        first_name || ' ' || last_name as full_name,
        last_name || ', ' || first_name as reversed_name,
        upper(first_name) || '_' || lower(last_name) as mixed_case_name,
        -- String functions
        upper(first_name) as upper_first,
        lower(last_name) as lower_last,
        initcap(first_name || ' ' || last_name) as proper_name,
        reverse(first_name) as reversed_first,
        reverse(last_name) as reversed_last,
        -- Length calculations
        length(first_name) as first_name_len,
        length(last_name) as last_name_len,
        length(first_name) + length(last_name) as total_name_len,
        -- Substring operations
        left(first_name, 1) as first_initial,
        left(last_name, 1) as last_initial,
        left(first_name, 1) || left(last_name, 1) as initials,
        substr(first_name, 1, 3) as first_three,
        -- Replace operations
        replace(first_name, 'a', '@') as first_with_at,
        replace(replace(first_name, 'a', '@'), 'e', '3') as leet_first,
        replace(replace(replace(first_name, 'a', '@'), 'e', '3'), 'i', '1') as leet_full,
        -- Hash operations (CPU intensive)
        md5(first_name) as first_name_md5,
        md5(last_name) as last_name_md5,
        md5(first_name || last_name) as full_name_md5,
        sha1(first_name) as first_name_sha1,
        sha1(last_name) as last_name_sha1,
        sha2(first_name || last_name, 256) as full_name_sha256
    from all_customers
),

-- ============================================================================
-- LAYER 9: Excessive window functions (multiple sorts)
-- ============================================================================

window_heavy_customers as (
    select
        customer_id,
        first_name,
        last_name,
        -- Row numbers with different orderings
        row_number() over (order by customer_id) as rn_by_id,
        row_number() over (order by first_name) as rn_by_first,
        row_number() over (order by last_name) as rn_by_last,
        row_number() over (order by customer_id desc) as rn_by_id_desc,
        -- Dense ranks
        dense_rank() over (order by customer_id) as dr_by_id,
        dense_rank() over (order by first_name) as dr_by_first,
        dense_rank() over (order by last_name) as dr_by_last,
        -- Ranks
        rank() over (order by customer_id) as rank_by_id,
        rank() over (order by first_name) as rank_by_first,
        -- Ntile partitions
        ntile(4) over (order by customer_id) as quartile,
        ntile(10) over (order by customer_id) as decile,
        ntile(100) over (order by customer_id) as percentile,
        -- Lag/Lead
        lag(customer_id, 1) over (order by customer_id) as prev_customer,
        lag(customer_id, 2) over (order by customer_id) as prev_prev_customer,
        lead(customer_id, 1) over (order by customer_id) as next_customer,
        lead(customer_id, 2) over (order by customer_id) as next_next_customer,
        -- First/Last value
        first_value(customer_id) over (order by first_name) as first_by_name,
        last_value(customer_id) over (order by first_name) as last_by_name
    from all_customers
),

-- ============================================================================
-- LAYER 10: UNION ALL instead of single aggregation (multiple passes)
-- ============================================================================

payment_method_counts as (
    select 'credit_card' as method, count(*) as cnt, sum(amount) as total 
    from all_payments where payment_method = 'credit_card'
    union all
    select 'coupon' as method, count(*) as cnt, sum(amount) as total 
    from all_payments where payment_method = 'coupon'
    union all
    select 'bank_transfer' as method, count(*) as cnt, sum(amount) as total 
    from all_payments where payment_method = 'bank_transfer'
    union all
    select 'gift_card' as method, count(*) as cnt, sum(amount) as total 
    from all_payments where payment_method = 'gift_card'
),

order_status_counts as (
    select 'placed' as status, count(*) as cnt 
    from all_orders where status = 'placed'
    union all
    select 'shipped' as status, count(*) as cnt 
    from all_orders where status = 'shipped'
    union all
    select 'completed' as status, count(*) as cnt 
    from all_orders where status = 'completed'
    union all
    select 'return_pending' as status, count(*) as cnt 
    from all_orders where status = 'return_pending'
    union all
    select 'returned' as status, count(*) as cnt 
    from all_orders where status = 'returned'
),

-- ============================================================================
-- LAYER 11: Self-joins (joining table to itself)
-- ============================================================================

customer_self_join as (
    select
        c1.customer_id as customer_1,
        c1.first_name as first_name_1,
        c2.customer_id as customer_2,
        c2.first_name as first_name_2,
        case when c1.first_name = c2.first_name then 1 else 0 end as same_first_name
    from all_customers c1
    inner join all_customers c2 on c1.customer_id <= c2.customer_id
),

order_self_join as (
    select
        o1.order_id as order_1,
        o1.customer_id as customer_1,
        o2.order_id as order_2,
        o2.customer_id as customer_2,
        datediff('day', o1.order_date, o2.order_date) as days_between
    from all_orders o1
    inner join all_orders o2 on o1.order_id < o2.order_id
),

-- ============================================================================
-- LAYER 12: Unused CTEs (dead code that still gets compiled)
-- ============================================================================

unused_cte_1 as (
    select count(*) as c from all_customers
),

unused_cte_2 as (
    select sum(amount) as s from all_payments
),

unused_cte_3 as (
    select max(order_date) as m from all_orders
),

-- ============================================================================
-- LAYER 13: Redundant aggregations (same calculation multiple ways)
-- ============================================================================

redundant_order_stats as (
    select
        customer_id,
        count(*) as order_count_1,
        count(order_id) as order_count_2,
        sum(1) as order_count_3,
        count(distinct order_id) as order_count_4,
        count(case when order_id is not null then 1 end) as order_count_5
    from all_orders
    group by customer_id
),

redundant_payment_stats as (
    select
        order_id,
        sum(amount) as total_1,
        sum(amount * 1) as total_2,
        sum(amount + 0) as total_3,
        sum(cast(amount as decimal(18,2))) as total_4,
        avg(amount) * count(*) as total_5
    from all_payments
    group by order_id
),

-- ============================================================================
-- LAYER 14: Massive final join combining everything
-- ============================================================================

mega_join as (
    select
        -- Customer base info
        c.customer_id,
        c.first_name,
        c.last_name,
        -- Order info
        o.order_id,
        o.order_date,
        o.status,
        -- Payment info
        p.payment_id,
        p.payment_method,
        p.amount,
        -- Correlated stats
        ccs.order_count as corr_order_count,
        ccs.latest_order_date as corr_latest_order,
        ccs.total_spent as corr_total_spent,
        ccs.avg_payment as corr_avg_payment,
        -- String heavy
        shc.full_name,
        shc.mixed_case_name,
        shc.leet_full,
        shc.full_name_md5,
        shc.full_name_sha256,
        -- Window heavy
        whc.rn_by_id,
        whc.dr_by_first,
        whc.quartile,
        whc.percentile,
        -- Redundant stats
        ros.order_count_1,
        ros.order_count_5,
        rps.total_1 as payment_total
    from all_customers c
    left join all_orders o on c.customer_id = o.customer_id
    left join all_payments p on o.order_id = p.order_id
    left join customer_with_correlated_stats ccs on c.customer_id = ccs.customer_id
    left join string_heavy_customers shc on c.customer_id = shc.customer_id
    left join window_heavy_customers whc on c.customer_id = whc.customer_id
    left join redundant_order_stats ros on c.customer_id = ros.customer_id
    left join redundant_payment_stats rps on o.order_id = rps.order_id
),

-- ============================================================================
-- LAYER 15: Final aggregation with excessive calculations
-- ============================================================================

final as (
    select
        customer_id,
        first_name,
        last_name,
        full_name,
        mixed_case_name,
        
        -- Count aggregations
        count(*) as total_rows,
        count(distinct order_id) as unique_orders,
        count(distinct payment_id) as unique_payments,
        count(distinct payment_method) as unique_payment_methods,
        count(distinct status) as unique_statuses,
        count(distinct order_date) as unique_order_dates,
        
        -- Sum aggregations
        sum(amount) as total_amount,
        sum(coalesce(amount, 0)) as total_amount_coalesced,
        
        -- Average aggregations
        avg(amount) as avg_amount,
        avg(coalesce(amount, 0)) as avg_amount_coalesced,
        
        -- Min/Max aggregations
        min(amount) as min_amount,
        max(amount) as max_amount,
        max(amount) - min(amount) as amount_range,
        
        -- Statistical aggregations
        stddev(amount) as stddev_amount,
        variance(amount) as var_amount,
        
        -- Date aggregations
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        datediff('day', min(order_date), max(order_date)) as customer_lifetime_days,
        
        -- From correlated stats
        max(corr_order_count) as correlated_order_count,
        max(corr_total_spent) as correlated_total_spent,
        max(corr_avg_payment) as correlated_avg_payment,
        
        -- Window function results
        max(rn_by_id) as max_row_num,
        max(quartile) as customer_quartile,
        max(percentile) as customer_percentile,
        
        -- Hash values
        max(full_name_md5) as name_hash_md5,
        max(full_name_sha256) as name_hash_sha256,
        
        -- Metadata
        current_timestamp() as processed_at,
        current_date() as processed_date,
        'v1.0' as model_version
        
    from mega_join
    group by 
        customer_id,
        first_name,
        last_name,
        full_name,
        mixed_case_name
    
    -- Anti-pattern: ORDER BY in table materialization (unnecessary sort)
    order by 
        total_amount desc nulls last,
        unique_orders desc,
        customer_id asc
)

select * from final
