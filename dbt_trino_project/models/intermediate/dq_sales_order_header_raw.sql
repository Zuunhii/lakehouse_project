{{ config(
    materialized='incremental',
    schema='silver',
    alias='dq_sales_order_header_raw',
    incremental_strategy='merge',
    unique_key=['etl_date','salesorderid']
) }}

{% set run_date = var('run_date', run_started_at.strftime('%Y-%m-%d')) %}

with base as (
    select
        salesorderid,
        customerid,
        creditcardid,
        shiptoaddressid,
        cast(status as bigint)     as status,
        cast(orderdate as date)    as orderdate,
        cast(totaldue as double)   as totaldue,
        cast(etl_date as date)     as etl_date
    from {{ source('bronze_src', 'bronze_sales_salesorderheader_LIVE') }}
    where etl_date = date '{{ run_date }}'
),

dups as (
    select etl_date, salesorderid, count(*) as cnt
    from base
    group by 1,2
),

scored as (
    select
        b.*,

        case when b.salesorderid is null or b.customerid is null then 1 else 0 end as f_null_key,
        case when b.orderdate is null then 1 else 0 end as f_orderdate_null,
        case when b.status is null then 1 else 0 end as f_status_null,
        case when b.totaldue is null or b.totaldue < 0 then 1 else 0 end as f_totaldue_invalid,
        case when d.cnt > 1 then 1 else 0 end as f_dup_key

    from base b
    left join dups d
      on b.etl_date = d.etl_date
     and b.salesorderid = d.salesorderid
)

select
    {{ dbt_utils.generate_surrogate_key(['etl_date','salesorderid']) }} as dq_row_wid,

    salesorderid,
    customerid,
    creditcardid,
    shiptoaddressid,
    status,
    orderdate,
    totaldue,
    etl_date,

    f_null_key,
    f_orderdate_null,
    f_status_null,
    f_totaldue_invalid,
    f_dup_key,

    (f_null_key + f_orderdate_null + f_status_null + f_totaldue_invalid + f_dup_key) as dq_score,

    case
        when f_null_key = 1 or f_dup_key = 1 then 'FAIL'
        when (f_orderdate_null + f_status_null + f_totaldue_invalid) >= 2 then 'FAIL'
        when (f_orderdate_null + f_status_null + f_totaldue_invalid) = 1 then 'WARN'
        else 'PASS'
    end as dq_class
from scored
