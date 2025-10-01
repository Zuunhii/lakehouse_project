select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      



select
    1
from "iceberg"."gold"."dim_credit_card"

where not(last4 length() = 4 and regexp_like(, '^[0-9]{4}$'))


      
    ) dbt_internal_test