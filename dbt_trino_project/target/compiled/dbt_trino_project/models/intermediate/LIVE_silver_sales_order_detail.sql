






with bronze_raw as (

    select
        salesorderid,
        salesorderdetailid,
        carriertrackingnumber,
        orderqty,
        productid,
        specialofferid,
        unitprice,
        unitpricediscount,
        linetotal,
        rowguid,
        modifieddate,

        -- metadata (tự thêm ở bronze/ingest)
        etl_date
    from "iceberg"."bronze"."bronze_sales_salesorderdetail_LIVE"

    -- ✅ LUÔN chỉ xử lý đúng ngày dữ liệu được truyền vào
    where etl_date = date 'None'
),

typed as (
    select
        try_cast(salesorderid as bigint)          as salesorderid,
        try_cast(salesorderdetailid as bigint)    as salesorderdetailid,

        nullif(trim(carriertrackingnumber), '')   as carriertrackingnumber,

        try_cast(orderqty as bigint)              as orderqty_raw,
        try_cast(productid as bigint)             as productid,
        try_cast(specialofferid as bigint)        as specialofferid,

        try_cast(unitprice as double)             as unitprice_raw,
        try_cast(unitpricediscount as double)     as discount_raw,
        try_cast(linetotal as double)             as linetotal_raw,

        rowguid,
        cast(modifieddate as timestamp(6))        as modifieddate,
        cast(etl_date as date)                    as etl_date
    from bronze_raw
),

clean as (
    select
        -- keys
        salesorderid,
        salesorderdetailid,
        productid,
        specialofferid,

        -- attrs
        carriertrackingnumber,
        rowguid,
        modifieddate,
        etl_date,

        -- measures: clean hard theo faker
        orderqty_raw as orderqty,

        -- unitprice âm -> ABS()
        abs(unitprice_raw) as unitprice,

        -- discount out-of-range -> clamp [0,1], null -> 0
        case
            when discount_raw is null then 0.0
            else greatest(0.0, least(1.0, discount_raw))
        end as unitpricediscount,

        -- linetotal: recompute chuẩn (ignore linetotal_raw vì faker cố tình làm lệch)
        round(
            abs(unitprice_raw) * orderqty_raw
            * (1 - (case
                    when discount_raw is null then 0.0
                    else greatest(0.0, least(1.0, discount_raw))
                  end)),
            2
        ) as linetotal

    from typed
    where
        -- ✅ sạch hẳn: chỉ giữ row đủ điều kiện để fact join thẳng
        salesorderid is not null
        and salesorderdetailid is not null
        and productid is not null
        and orderqty_raw is not null
        and orderqty_raw > 0
        and unitprice_raw is not null
)

select *
from clean