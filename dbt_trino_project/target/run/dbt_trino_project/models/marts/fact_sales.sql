
  
    

    create table "iceberg"."gold"."fact_sales__dbt_tmp"
      
      
    as (
      -- models/marts/fact_sales.sql


with stg_salesorderheader as (
    select
        salesorderid,
        customerid,
        creditcardid,
        shiptoaddressid,
        cast(status as bigint)       as order_status,
        cast(orderdate as date)      as orderdate,
        totaldue
    from "iceberg"."silver"."silver_sales_salesorderheader"
),

stg_salesorderdetail as (
    select
        salesorderid,
        salesorderdetailid,
        productid,
        orderqty,
        unitprice,
        (unitprice * orderqty) as revenue
    from "iceberg"."silver"."silver_sales_salesorderdetail"
)

select
    -- Grain: 1 row / (salesorderid, salesorderdetailid)
    lower(to_hex(md5(to_utf8(cast(coalesce(cast(d.salesorderid as varchar), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(d.salesorderdetailid as varchar), '_dbt_utils_surrogate_key_null_') as varchar))))) as sales_id_WID,

    -- FKs sang dims (hash từ natural keys)
    lower(to_hex(md5(to_utf8(cast(coalesce(cast(d.productid as varchar), '_dbt_utils_surrogate_key_null_') as varchar)))))        as product_id_WID,
    lower(to_hex(md5(to_utf8(cast(coalesce(cast(h.customerid as varchar), '_dbt_utils_surrogate_key_null_') as varchar)))))       as customer_id_WID,
    lower(to_hex(md5(to_utf8(cast(coalesce(cast(h.creditcardid as varchar), '_dbt_utils_surrogate_key_null_') as varchar)))))     as credit_card_id_WID,
    lower(to_hex(md5(to_utf8(cast(coalesce(cast(h.shiptoaddressid as varchar), '_dbt_utils_surrogate_key_null_') as varchar)))))  as ship_address_id_WID,
    lower(to_hex(md5(to_utf8(cast(coalesce(cast(h.order_status as varchar), '_dbt_utils_surrogate_key_null_') as varchar)))))     as order_status_id_WID,
    lower(to_hex(md5(to_utf8(cast(coalesce(cast(h.orderdate as varchar), '_dbt_utils_surrogate_key_null_') as varchar)))))        as order_date_id_WID,

    -- Natural keys / trace
    d.salesorderid,
    d.salesorderdetailid,
    d.productid,
    h.customerid,
    h.creditcardid,
    h.shiptoaddressid,
    h.order_status,
    h.orderdate,

    -- Measures
    d.unitprice,
    d.orderqty,
    d.revenue,
    h.totaldue

from stg_salesorderdetail d
join stg_salesorderheader h
  on d.salesorderid = h.salesorderid
    );

  