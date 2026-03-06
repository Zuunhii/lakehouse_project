{{ config(
    materialized='incremental',
    schema='silver',
    alias='dq_sales_order_detail_raw',
    incremental_strategy='merge',
    unique_key=['etl_date','salesorderid','salesorderdetailid','dup_seq']
) }}

{% set run_date = var('run_date', run_started_at.strftime('%Y-%m-%d')) %}

with base as (
    select
        salesorderid,
        salesorderdetailid,
        productid,

        try_cast(orderqty as bigint)           as orderqty,
        try_cast(unitprice as double)          as unitprice,
        try_cast(unitpricediscount as double)  as unitpricediscount,
        try_cast(linetotal as double)          as linetotal,

        cast(etl_date as date)                 as etl_date,

        row_number() over (
          partition by cast(etl_date as date), salesorderid, salesorderdetailid
          order by productid, orderqty, unitprice, unitpricediscount, linetotal
        ) as dup_seq
    from {{ source('bronze_src', 'bronze_sales_salesorderdetail_LIVE') }}
    where etl_date = date '{{ run_date }}'
),


dups as (
    select etl_date, salesorderid, salesorderdetailid, count(*) as cnt
    from base
    group by 1,2,3
),

scored as (
    select
        b.*,

        -- key null
        case
            when b.salesorderid is null
              or b.salesorderdetailid is null
              or b.productid is null
            then 1 else 0
        end as f_null_key,

        -- basic validity
        case when b.orderqty is null or b.orderqty <= 0 then 1 else 0 end as f_qty_invalid,
        case when b.unitprice is null or b.unitprice < 0 then 1 else 0 end as f_price_invalid,

        -- discount validity (range [0,1], null treated as invalid for RAW)
        case
            when b.unitpricediscount is null then 1
            when b.unitpricediscount < 0 or b.unitpricediscount > 1 then 1
            else 0
        end as f_discount_invalid,

        case when b.linetotal is null or b.linetotal < 0 then 1 else 0 end as f_linetotal_invalid,

        -- ✅ linetotal đúng công thức: qty * price * (1 - discount)
        -- RAW: clamp discount để tránh 1.2/-0.1 làm công thức “điên”, nhưng vẫn có flag f_discount_invalid riêng
        case
            when b.orderqty is null
              or b.unitprice is null
              or b.unitpricediscount is null
              or b.linetotal is null
            then 0
            when abs(
                b.linetotal
                - round(
                    b.unitprice * b.orderqty
                    * (1 - greatest(0.0, least(1.0, b.unitpricediscount))),
                    2
                  )
            ) > 0.01
            then 1
            else 0
        end as f_linetotal_mismatch,

        -- dup key
        case when d.cnt > 1 then 1 else 0 end as f_dup_key

    from base b
    left join dups d
      on b.etl_date = d.etl_date
     and b.salesorderid = d.salesorderid
     and b.salesorderdetailid = d.salesorderdetailid
)

select
    {{ dbt_utils.generate_surrogate_key(['etl_date','salesorderid','salesorderdetailid','dup_seq']) }} as dq_row_wid,

    salesorderid,
    salesorderdetailid,
    productid,
    orderqty,
    unitprice,
    unitpricediscount,
    linetotal,
    etl_date,
    dup_seq,
    f_null_key,
    f_qty_invalid,
    f_price_invalid,
    f_discount_invalid,
    f_linetotal_invalid,
    f_linetotal_mismatch,
    f_dup_key,

    (f_null_key + f_qty_invalid + f_price_invalid + f_discount_invalid + f_linetotal_invalid + f_linetotal_mismatch + f_dup_key) as dq_score,

    case
        when f_null_key = 1
          or f_dup_key = 1
          or (f_qty_invalid + f_price_invalid + f_discount_invalid + f_linetotal_invalid + f_linetotal_mismatch) >= 2
        then 'FAIL'
        when (f_qty_invalid + f_price_invalid + f_discount_invalid + f_linetotal_invalid + f_linetotal_mismatch) = 1
        then 'WARN'
        else 'PASS'
    end as dq_class
from scored
