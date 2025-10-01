select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      



select
    1
from "iceberg"."gold"."dim_credit_card"

where not(expmonth expmonth between 1 and 12)


      
    ) dbt_internal_test