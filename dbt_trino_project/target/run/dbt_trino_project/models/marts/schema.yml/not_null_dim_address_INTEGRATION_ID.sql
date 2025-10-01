select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select INTEGRATION_ID
from "iceberg"."gold"."dim_address"
where INTEGRATION_ID is null



      
    ) dbt_internal_test