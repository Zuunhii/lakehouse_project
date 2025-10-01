select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select expmonth
from "iceberg"."gold"."dim_credit_card"
where expmonth is null



      
    ) dbt_internal_test