{{ config(
    materialized='incremental',
    schema='gold',
    alias='fact_sales_LIVE',
    incremental_strategy='merge',
    unique_key=['salesorderid','salesorderdetailid']
) }}

{% set run_date = var('run_date', none) %}

{% if flags.WHICH in ['run', 'build', 'test'] and run_date is none %}
  {{ exceptions.raise_compiler_error(
      "Missing required var: run_date (YYYY-MM-DD)"
  ) }}
{% endif %}
with h as (
    select
        salesorderid,
        customerid,
        creditcardid,
        shiptoaddressid,
        cast(status as bigint) as order_status,
        cast(orderdate as date) as orderdate,
        totaldue,
        etl_date
    from {{ ref('LIVE_silver_sales_order_header') }}
    where etl_date = date '{{ run_date }}'
),

d as (
    select
        salesorderid,
        salesorderdetailid,
        productid,
        orderqty,
        unitprice,
        linetotal,
        etl_date
    from {{ ref('LIVE_silver_sales_order_detail') }}
    where etl_date = date '{{ run_date }}'
),

joined as (
    select
        d.salesorderid,
        d.salesorderdetailid,
        d.productid,
        d.orderqty,
        d.unitprice,
        d.linetotal,

        h.customerid,
        h.creditcardid,
        h.shiptoaddressid,
        h.order_status,
        h.orderdate,
        h.totaldue,

        -- batch trace
        h.etl_date
    from d
    join h
      on d.salesorderid = h.salesorderid
)

select
    -- Grain: 1 row / (salesorderid, salesorderdetailid)
    {{ dbt_utils.generate_surrogate_key(['salesorderid','salesorderdetailid']) }} as sales_id_wid,

    -- FK WIDs
    {{ dbt_utils.generate_surrogate_key(['productid']) }}       as product_id_wid,
    {{ dbt_utils.generate_surrogate_key(['customerid']) }}      as customer_id_wid,
    {{ dbt_utils.generate_surrogate_key(['creditcardid']) }}    as credit_card_id_wid,
    {{ dbt_utils.generate_surrogate_key(['shiptoaddressid']) }} as ship_address_id_wid,
    {{ dbt_utils.generate_surrogate_key(['order_status']) }}    as order_status_id_wid,
    {{ dbt_utils.generate_surrogate_key(['orderdate']) }}       as order_date_id_wid,

    -- Natural keys
    salesorderid,
    salesorderdetailid,
    orderdate,

    -- Measures
    unitprice,
    orderqty,

    -- revenue: dùng linetotal (đã clean + recompute ở detail silver)
    linetotal as revenue,

    -- header totaldue (lặp theo từng line; ok nếu mày chỉ cần trace / hoặc sẽ allocate sau)
    totaldue,

    -- batch
    etl_date

from joined
