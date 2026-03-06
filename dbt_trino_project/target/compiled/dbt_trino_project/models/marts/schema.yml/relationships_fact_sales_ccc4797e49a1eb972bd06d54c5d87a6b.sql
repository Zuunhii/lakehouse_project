
    
    

with child as (
    select order_status_id_WID as from_field
    from "iceberg"."gold"."fact_sales"
    where order_status_id_WID is not null
),

parent as (
    select order_status_id_WID as to_field
    from "iceberg"."gold"."dim_order_status"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


