select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select addressline1
from "iceberg"."gold"."dim_address"
where addressline1 is null



      
    ) dbt_internal_test