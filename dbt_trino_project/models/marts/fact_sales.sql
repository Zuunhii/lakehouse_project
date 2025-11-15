-- models/marts/fact_sales.sql
{{ config(materialized='table') }}

with stg_salesorderheader as (
    select
        salesorderid,
        customerid,
        creditcardid,
        shiptoaddressid,
        cast(status as bigint)       as order_status,
        cast(orderdate as date)      as orderdate,
        totaldue
    from {{ source('silver_src', 'silver_sales_salesorderheader') }}
),

stg_salesorderdetail as (
    select
        salesorderid,
        salesorderdetailid,
        productid,
        orderqty,
        unitprice,
        (unitprice * orderqty) as revenue
    from {{ source('silver_src', 'silver_sales_salesorderdetail') }}
)

select
    -- Grain: 1 row / (salesorderid, salesorderdetailid)
    {{ dbt_utils.generate_surrogate_key(['d.salesorderid','d.salesorderdetailid']) }} as sales_id_WID,

    -- FKs sang dims (hash từ natural keys)
    {{ dbt_utils.generate_surrogate_key(['d.productid']) }}        as product_id_WID,
    {{ dbt_utils.generate_surrogate_key(['h.customerid']) }}       as customer_id_WID,
    {{ dbt_utils.generate_surrogate_key(['h.creditcardid']) }}     as credit_card_id_WID,
    {{ dbt_utils.generate_surrogate_key(['h.shiptoaddressid']) }}  as ship_address_id_WID,
    {{ dbt_utils.generate_surrogate_key(['h.order_status']) }}     as order_status_id_WID,
    {{ dbt_utils.generate_surrogate_key(['h.orderdate']) }}        as order_date_id_WID,

    -- Natural keys / trace
    d.salesorderid AS sales_order_id,
    d.salesorderdetailid AS sales_order_detail_id,
    -- d.productid,
    -- h.customerid,
    -- h.creditcardid,
    -- h.shiptoaddressid,
    -- h.order_status,
    h.orderdate,

    -- Measures
    d.unitprice,
    d.orderqty,
    d.revenue,
    h.totaldue

from stg_salesorderdetail d
join stg_salesorderheader h
  on d.salesorderid = h.salesorderid
