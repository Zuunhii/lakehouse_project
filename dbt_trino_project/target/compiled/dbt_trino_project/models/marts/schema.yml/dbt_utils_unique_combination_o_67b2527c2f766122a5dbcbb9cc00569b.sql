





with validation_errors as (

    select
        salesorderid, salesorderdetailid, etl_date
    from "iceberg"."gold"."fact_sales_LIVE"
    group by salesorderid, salesorderdetailid, etl_date
    having count(*) > 1

)

select *
from validation_errors


