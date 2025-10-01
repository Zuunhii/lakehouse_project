select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    product_id_WID as unique_field,
    count(*) as n_records

from "iceberg"."gold"."dim_product"
where product_id_WID is not null
group by product_id_WID
having count(*) > 1



      
    ) dbt_internal_test