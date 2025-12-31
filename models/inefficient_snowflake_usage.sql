{{
    config(
        materialized='table',
        tags=['usage', 'optimization_candidate']
    )
}}

/*
    ⚠️ INEFFICIENT MODEL FOR OPTIMIZATION DEMONSTRATION ⚠️
    
    This model queries Snowflake Account Usage views with multiple anti-patterns:
    - SELECT * from large views
    - No date filtering (scans entire history)
    - Multiple redundant scans of same views
    - Cross joins creating massive intermediate results
    - Expensive string operations
    - Correlated subqueries
    
    ACCOUNT_USAGE views retain data for 1 year - this query scans ALL of it!
*/

-- Anti-pattern: SELECT * from large usage views without date filtering
with all_query_history as (
    select *
    from snowflake.account_usage.query_history
    -- No WHERE clause = scans entire year of query history!
),

all_warehouse_metering as (
    select *
    from snowflake.account_usage.warehouse_metering_history
),

all_storage_usage as (
    select *
    from snowflake.account_usage.storage_usage
),

all_login_history as (
    select *
    from snowflake.account_usage.login_history
),

all_access_history as (
    select *
    from snowflake.account_usage.access_history
),

-- Anti-pattern: Redundant scans of the same views
query_history_scan_2 as (
    select *
    from snowflake.account_usage.query_history
),

warehouse_metering_scan_2 as (
    select *
    from snowflake.account_usage.warehouse_metering_history
),

-- Anti-pattern: Non-sargable WHERE clauses (functions on columns)
filtered_queries as (
    select *
    from all_query_history
    where upper(query_type) in ('SELECT', 'INSERT', 'UPDATE', 'DELETE')
       or lower(warehouse_name) like '%compute%'
       or trim(user_name) is not null
       or length(query_text) > 100
       or position('SELECT' in upper(query_text)) > 0
),

-- Anti-pattern: Expensive string operations on query text
query_text_analysis as (
    select
        query_id,
        query_text,
        user_name,
        warehouse_name,
        execution_time,
        -- Expensive string operations
        length(query_text) as query_length,
        upper(query_text) as query_upper,
        lower(query_text) as query_lower,
        md5(query_text) as query_hash,
        sha1(query_text) as query_sha,
        replace(query_text, 'SELECT', '[SELECT]') as query_highlighted,
        regexp_count(query_text, 'JOIN') as join_count,
        regexp_count(query_text, 'WHERE') as where_count,
        regexp_count(query_text, 'SELECT') as select_count
    from all_query_history
    where query_text is not null
),

-- Anti-pattern: Window functions causing multiple sorts
query_rankings as (
    select
        query_id,
        user_name,
        warehouse_name,
        execution_time,
        total_elapsed_time,
        bytes_scanned,
        rows_produced,
        -- Multiple window functions with different orderings
        row_number() over (order by execution_time desc) as exec_time_rank,
        row_number() over (order by total_elapsed_time desc) as elapsed_rank,
        row_number() over (order by bytes_scanned desc nulls last) as bytes_rank,
        row_number() over (partition by user_name order by execution_time desc) as user_exec_rank,
        row_number() over (partition by warehouse_name order by execution_time desc) as wh_exec_rank,
        dense_rank() over (order by execution_time desc) as exec_dense_rank,
        ntile(100) over (order by execution_time desc) as exec_percentile,
        sum(execution_time) over (partition by user_name) as user_total_exec,
        sum(bytes_scanned) over (partition by warehouse_name) as wh_total_bytes,
        avg(execution_time) over (partition by user_name) as user_avg_exec
    from all_query_history
),

-- Anti-pattern: Correlated subqueries
user_stats_correlated as (
    select
        distinct user_name,
        (select count(*) from all_query_history q2 
         where q2.user_name = q.user_name) as query_count,
        (select sum(execution_time) from all_query_history q2 
         where q2.user_name = q.user_name) as total_exec_time,
        (select avg(bytes_scanned) from all_query_history q2 
         where q2.user_name = q.user_name) as avg_bytes,
        (select max(start_time) from all_query_history q2 
         where q2.user_name = q.user_name) as last_query_time
    from all_query_history q
),

-- Anti-pattern: Cross join creating massive intermediate results
user_warehouse_matrix as (
    select
        u.user_name,
        w.warehouse_name,
        w.credits_used
    from (select distinct user_name from all_query_history) u
    cross join all_warehouse_metering w
),

-- Anti-pattern: UNION ALL instead of single aggregation
warehouse_credits_by_type as (
    select 'COMPUTE' as category, sum(credits_used) as credits
    from all_warehouse_metering where warehouse_name like '%COMPUTE%'
    union all
    select 'LOADING' as category, sum(credits_used) as credits
    from all_warehouse_metering where warehouse_name like '%LOAD%'
    union all
    select 'TRANSFORM' as category, sum(credits_used) as credits
    from all_warehouse_metering where warehouse_name like '%TRANSFORM%'
    union all
    select 'OTHER' as category, sum(credits_used) as credits
    from all_warehouse_metering 
    where warehouse_name not like '%COMPUTE%' 
      and warehouse_name not like '%LOAD%' 
      and warehouse_name not like '%TRANSFORM%'
),

-- Anti-pattern: Multiple aggregations at different granularities joined together
daily_query_stats as (
    select
        date_trunc('day', start_time) as query_date,
        count(*) as query_count,
        sum(execution_time) as total_exec_ms,
        sum(bytes_scanned) as total_bytes,
        count(distinct user_name) as unique_users,
        count(distinct warehouse_name) as unique_warehouses
    from all_query_history
    group by 1
),

hourly_query_stats as (
    select
        date_trunc('hour', start_time) as query_hour,
        count(*) as query_count,
        sum(execution_time) as total_exec_ms
    from query_history_scan_2
    group by 1
),

-- Anti-pattern: Self-join on large table
query_sequence as (
    select
        q1.query_id as query1,
        q2.query_id as query2,
        q1.user_name,
        datediff('second', q1.start_time, q2.start_time) as seconds_between
    from all_query_history q1
    inner join query_history_scan_2 q2 
        on q1.user_name = q2.user_name 
        and q1.query_id < q2.query_id
),

-- Anti-pattern: Mega join combining everything
mega_usage_join as (
    select
        qh.query_id,
        qh.query_text,
        qh.user_name,
        qh.warehouse_name,
        qh.execution_time,
        qh.total_elapsed_time,
        qh.bytes_scanned,
        qh.rows_produced,
        qh.start_time,
        qh.end_time,
        qh.query_type,
        qh.session_id,
        qh.compilation_time,
        qh.queued_provisioning_time,
        qh.queued_overload_time,
        -- From text analysis
        qta.query_length,
        qta.query_hash,
        qta.join_count,
        qta.where_count,
        -- From rankings
        qr.exec_time_rank,
        qr.elapsed_rank,
        qr.bytes_rank,
        qr.exec_percentile,
        qr.user_total_exec,
        -- From correlated stats
        usc.query_count as user_query_count,
        usc.total_exec_time as user_total_time,
        -- From daily stats
        dqs.query_count as daily_queries,
        dqs.unique_users as daily_users
    from all_query_history qh
    left join query_text_analysis qta on qh.query_id = qta.query_id
    left join query_rankings qr on qh.query_id = qr.query_id
    left join user_stats_correlated usc on qh.user_name = usc.user_name
    left join daily_query_stats dqs on date_trunc('day', qh.start_time) = dqs.query_date
),

-- Final aggregation with excessive calculations
final as (
    select
        user_name,
        warehouse_name,
        query_type,
        -- Counts
        count(*) as total_queries,
        count(distinct query_id) as unique_queries,
        count(distinct session_id) as unique_sessions,
        count(distinct date_trunc('day', start_time)) as active_days,
        -- Execution metrics
        sum(execution_time) as total_execution_ms,
        avg(execution_time) as avg_execution_ms,
        min(execution_time) as min_execution_ms,
        max(execution_time) as max_execution_ms,
        stddev(execution_time) as stddev_execution_ms,
        -- Elapsed time metrics  
        sum(total_elapsed_time) as total_elapsed_ms,
        avg(total_elapsed_time) as avg_elapsed_ms,
        -- Bytes metrics
        sum(bytes_scanned) as total_bytes_scanned,
        avg(bytes_scanned) as avg_bytes_scanned,
        max(bytes_scanned) as max_bytes_scanned,
        -- Rows metrics
        sum(rows_produced) as total_rows_produced,
        avg(rows_produced) as avg_rows_produced,
        -- Compilation metrics
        sum(compilation_time) as total_compilation_ms,
        avg(compilation_time) as avg_compilation_ms,
        -- Queue metrics
        sum(queued_provisioning_time) as total_queue_provision_ms,
        sum(queued_overload_time) as total_queue_overload_ms,
        -- Query text stats
        avg(query_length) as avg_query_length,
        max(query_length) as max_query_length,
        sum(join_count) as total_joins_used,
        sum(where_count) as total_wheres_used,
        -- Percentile info
        max(exec_percentile) as worst_percentile,
        -- Time range
        min(start_time) as first_query_time,
        max(start_time) as last_query_time,
        datediff('day', min(start_time), max(start_time)) as active_period_days,
        -- Metadata
        current_timestamp() as analysis_timestamp,
        'v1.0' as model_version
    from mega_usage_join
    group by user_name, warehouse_name, query_type
    -- Anti-pattern: ORDER BY in table materialization
    order by total_execution_ms desc nulls last
)

select * from final
