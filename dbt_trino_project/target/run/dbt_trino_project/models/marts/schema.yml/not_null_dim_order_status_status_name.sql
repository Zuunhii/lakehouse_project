select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select status_name
from "iceberg"."gold"."dim_order_status"
where status_name is null



      
    ) dbt_internal_test