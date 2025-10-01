
    
    

select
    product_id_WID as unique_field,
    count(*) as n_records

from "iceberg"."gold"."dim_product"
where product_id_WID is not null
group by product_id_WID
having count(*) > 1


