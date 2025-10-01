select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select product_id_WID
from "iceberg"."gold"."dim_product"
where product_id_WID is null



      
    ) dbt_internal_test