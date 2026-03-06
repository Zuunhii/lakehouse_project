-- =========================================
-- 1) dq_compare_salesorderdetail_live.sql  (FIXED)
--   - Compare theo KEY-LEVEL (aggregate) để không vỡ khi có duplicate
--   - Giữ unique_key 3 cột vì giờ 1 key = 1 row
-- =========================================
{{ config(
    materialized='incremental',
    schema='silver',
    alias='dq_compare_salesorderdetail_live',
    incremental_strategy='merge',
    unique_key=['etl_date','salesorderid','salesorderdetailid']
) }}

{% set run_date = var('run_date', none) %}
{% if run_date is none or run_date == 'None' or run_date == '' %}
  {% set run_date = run_started_at.strftime('%Y-%m-%d') %}
{% endif %}

with r as (
    select
        etl_date,
        salesorderid,
        salesorderdetailid,
        count(*) as raw_row_cnt,
        max(dq_score) as raw_score_max,
        max(case dq_class when 'PASS' then 0 when 'WARN' then 1 when 'FAIL' then 2 else -1 end) as raw_class_rank
    from {{ ref('dq_sales_order_detail_raw') }}
    where etl_date = date '{{ run_date }}'
    group by 1,2,3
),
c as (
    select
        etl_date,
        salesorderid,
        salesorderdetailid,
        count(*) as clean_row_cnt,
        max(dq_score) as clean_score_max,
        max(case dq_class when 'PASS' then 0 when 'WARN' then 1 when 'FAIL' then 2 else -1 end) as clean_class_rank
    from {{ ref('dq_sales_order_detail_clean') }}
    where etl_date = date '{{ run_date }}'
    group by 1,2,3
),
j as (
    select
        coalesce(r.etl_date, c.etl_date) as etl_date,
        coalesce(r.salesorderid, c.salesorderid) as salesorderid,
        coalesce(r.salesorderdetailid, c.salesorderdetailid) as salesorderdetailid,

        r.raw_row_cnt,
        c.clean_row_cnt,
        (coalesce(r.raw_row_cnt, 0) - coalesce(c.clean_row_cnt, 0)) as row_cnt_delta,

        case r.raw_class_rank when 2 then 'FAIL' when 1 then 'WARN' when 0 then 'PASS' else null end as raw_class,
        case c.clean_class_rank when 2 then 'FAIL' when 1 then 'WARN' when 0 then 'PASS' else null end as clean_class,

        r.raw_score_max as raw_score,
        c.clean_score_max as clean_score,
        (coalesce(r.raw_score_max, 0) - coalesce(c.clean_score_max, 0)) as score_delta,

        case
            when r.raw_class_rank is null and c.clean_class_rank is not null then 'NEW_IN_CLEAN'
            when r.raw_class_rank is not null and c.clean_class_rank is null then 'DROPPED_IN_CLEAN'
            when r.raw_class_rank = c.clean_class_rank then 'UNCHANGED'
            when r.raw_class_rank > c.clean_class_rank then 'IMPROVED'
            when r.raw_class_rank < c.clean_class_rank then 'WORSENED'
            else 'CHANGED'
        end as improve_class
    from r
    full outer join c
      on r.etl_date = c.etl_date
     and r.salesorderid = c.salesorderid
     and r.salesorderdetailid = c.salesorderdetailid
)

select
    {{ dbt_utils.generate_surrogate_key(['etl_date','salesorderid','salesorderdetailid']) }} as compare_row_wid,
    *
from j

