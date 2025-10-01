select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      



select
    1
from "iceberg"."gold"."dim_credit_card"

where not(expyear between 2000 and 2100)


      
    ) dbt_internal_test