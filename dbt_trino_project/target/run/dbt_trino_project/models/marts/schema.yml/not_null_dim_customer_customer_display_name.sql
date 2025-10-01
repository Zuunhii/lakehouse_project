select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select customer_display_name
from "iceberg"."gold"."dim_customer"
where customer_display_name is null



      
    ) dbt_internal_test