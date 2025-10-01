select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      



select
    1
from "iceberg"."gold"."dim_credit_card"

where not(last4 length(last4) = 4 and regexp_like(last4, '^[0-9]{4}$'))


      
    ) dbt_internal_test