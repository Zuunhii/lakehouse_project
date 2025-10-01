select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select expyear
from "iceberg"."gold"."dim_credit_card"
where expyear is null



      
    ) dbt_internal_test