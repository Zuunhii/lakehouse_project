





with validation_errors as (

    select
        order_id_WID
    from "iceberg"."gold"."fact_sales"
    group by order_id_WID
    having count(*) > 1

)

select *
from validation_errors


