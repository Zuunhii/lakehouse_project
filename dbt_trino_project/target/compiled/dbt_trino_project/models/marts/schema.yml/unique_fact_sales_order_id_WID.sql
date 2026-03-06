
    
    

select
    order_id_WID as unique_field,
    count(*) as n_records

from "iceberg"."gold"."fact_sales"
where order_id_WID is not null
group by order_id_WID
having count(*) > 1


