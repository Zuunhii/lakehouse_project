




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
    from "iceberg"."silver"."silver_sales_salesorderheader_LIVE"
    where etl_date = date 'None'
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
    from "iceberg"."silver"."silver_sales_salesorderdetail_LIVE"
    where etl_date = date 'None'
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
    lower(to_hex(md5(to_utf8(cast(coalesce(cast(salesorderid as varchar), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(salesorderdetailid as varchar), '_dbt_utils_surrogate_key_null_') as varchar))))) as sales_id_wid,

    -- FK WIDs
    lower(to_hex(md5(to_utf8(cast(coalesce(cast(productid as varchar), '_dbt_utils_surrogate_key_null_') as varchar)))))       as product_id_wid,
    lower(to_hex(md5(to_utf8(cast(coalesce(cast(customerid as varchar), '_dbt_utils_surrogate_key_null_') as varchar)))))      as customer_id_wid,
    lower(to_hex(md5(to_utf8(cast(coalesce(cast(creditcardid as varchar), '_dbt_utils_surrogate_key_null_') as varchar)))))    as credit_card_id_wid,
    lower(to_hex(md5(to_utf8(cast(coalesce(cast(shiptoaddressid as varchar), '_dbt_utils_surrogate_key_null_') as varchar))))) as ship_address_id_wid,
    lower(to_hex(md5(to_utf8(cast(coalesce(cast(order_status as varchar), '_dbt_utils_surrogate_key_null_') as varchar)))))    as order_status_id_wid,
    lower(to_hex(md5(to_utf8(cast(coalesce(cast(orderdate as varchar), '_dbt_utils_surrogate_key_null_') as varchar)))))       as order_date_id_wid,

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