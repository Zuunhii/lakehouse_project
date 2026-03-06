-- =========================================
-- dq_compare_salesorderheader_live.sql
--   - Compare RAW vs CLEAN theo KEY-LEVEL (aggregate) để không vỡ khi duplicate
--   - 1 key (etl_date, salesorderid) = 1 row output
-- =========================================




  


with r as (
    select
        etl_date,
        salesorderid,
        count(*) as raw_row_cnt,
        max(dq_score) as raw_score_max,
        max(case dq_class when 'PASS' then 0 when 'WARN' then 1 when 'FAIL' then 2 else -1 end) as raw_class_rank
    from "iceberg"."silver"."dq_sales_order_header_raw"
    where etl_date = date '2026-01-09'
    group by 1,2
),
c as (
    select
        etl_date,
        salesorderid,
        count(*) as clean_row_cnt,
        max(dq_score) as clean_score_max,
        max(case dq_class when 'PASS' then 0 when 'WARN' then 1 when 'FAIL' then 2 else -1 end) as clean_class_rank
    from "iceberg"."silver"."dq_sales_order_header_clean"
    where etl_date = date '2026-01-09'
    group by 1,2
),
j as (
    select
        coalesce(r.etl_date, c.etl_date) as etl_date,
        coalesce(r.salesorderid, c.salesorderid) as salesorderid,

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
)

select
    lower(to_hex(md5(to_utf8(cast(coalesce(cast(etl_date as varchar), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(salesorderid as varchar), '_dbt_utils_surrogate_key_null_') as varchar))))) as compare_row_wid,
    *
from j