{{ config(
    materialized='table',
    schema='gold',
    alias='dq_agent_report_daily'
) }}

select
  cast(null as date)         as etl_date,
  cast(null as varchar)      as object_name,
  cast(null as integer)      as window_days,
  cast(null as bigint)       as raw_cnt,
  cast(null as bigint)       as clean_cnt,
  cast(null as bigint)       as fail_cnt,
  cast(null as bigint)       as warn_cnt,
  cast(null as bigint)       as pass_cnt,
  cast(null as bigint)       as delta_raw_clean,
  cast(null as varchar)      as top_issues,
  cast(null as varchar)      as compare_changes,
  cast(null as varchar)      as narrative,
  cast(null as varchar)      as drilldown_sql_1,
  cast(null as varchar)      as drilldown_sql_2,
  cast(null as varchar)      as drilldown_sql_3,
  cast(null as timestamp(6)) as created_at
where 1=0
