-- =========================================
-- 2) dq_compare_daily_summary.sql (FIXED)
--   - Bổ sung discount_invalid_cnt cho detail raw/clean
--   - Giữ nguyên cấu trúc union all
-- =========================================




  


with detail_raw as (
    select
        etl_date,
        'detail' as object_name,
        'raw' as dataset,
        dq_class,
        count(*) as row_cnt,
        sum(f_null_key) as null_key_cnt,
        sum(f_qty_invalid) as qty_invalid_cnt,
        sum(f_price_invalid) as price_invalid_cnt,
        sum(f_discount_invalid) as discount_invalid_cnt,
        sum(f_linetotal_invalid) as linetotal_invalid_cnt,
        sum(f_linetotal_mismatch) as linetotal_mismatch_cnt,
        sum(f_dup_key) as dup_key_cnt
    from "iceberg"."silver"."dq_sales_order_detail_raw"
    where etl_date = date '2026-01-09'
    group by 1,2,3,4
),
detail_clean as (
    select
        etl_date,
        'detail' as object_name,
        'clean' as dataset,
        dq_class,
        count(*) as row_cnt,
        sum(f_null_key) as null_key_cnt,
        sum(f_qty_invalid) as qty_invalid_cnt,
        sum(f_price_invalid) as price_invalid_cnt,
        sum(f_discount_invalid) as discount_invalid_cnt,
        sum(f_linetotal_invalid) as linetotal_invalid_cnt,
        sum(f_linetotal_mismatch) as linetotal_mismatch_cnt,
        sum(f_dup_key) as dup_key_cnt
    from "iceberg"."silver"."dq_sales_order_detail_clean"
    where etl_date = date '2026-01-09'
    group by 1,2,3,4
),
header_raw as (
    select
        etl_date,
        'header' as object_name,
        'raw' as dataset,
        dq_class,
        count(*) as row_cnt,
        sum(f_null_key) as null_key_cnt,
        0 as qty_invalid_cnt,
        0 as price_invalid_cnt,
        0 as discount_invalid_cnt,
        0 as linetotal_invalid_cnt,
        0 as linetotal_mismatch_cnt,
        sum(f_dup_key) as dup_key_cnt
    from "iceberg"."silver"."dq_sales_order_header_raw"
    where etl_date = date '2026-01-09'
    group by 1,2,3,4
),
header_clean as (
    select
        etl_date,
        'header' as object_name,
        'clean' as dataset,
        dq_class,
        count(*) as row_cnt,
        sum(f_null_key) as null_key_cnt,
        0 as qty_invalid_cnt,
        0 as price_invalid_cnt,
        0 as discount_invalid_cnt,
        0 as linetotal_invalid_cnt,
        0 as linetotal_mismatch_cnt,
        sum(f_dup_key) as dup_key_cnt
    from "iceberg"."silver"."dq_sales_order_header_clean"
    where etl_date = date '2026-01-09'
    group by 1,2,3,4
)

select * from detail_raw
union all
select * from detail_clean
union all
select * from header_raw
union all
select * from header_clean