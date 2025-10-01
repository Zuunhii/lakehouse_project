
    
    

with all_values as (

    select
        INTEGRATION_ID as value_field,
        count(*) as n_records

    from "iceberg"."gold"."dim_order_status"
    group by INTEGRATION_ID

)

select *
from all_values
where value_field not in (
    '1','2','3','4','5','6'
)


