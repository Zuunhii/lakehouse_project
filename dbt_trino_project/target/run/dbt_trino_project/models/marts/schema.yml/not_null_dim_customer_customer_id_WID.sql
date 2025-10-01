select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select customer_id_WID
from "iceberg"."gold"."dim_customer"
where customer_id_WID is null



      
    ) dbt_internal_test