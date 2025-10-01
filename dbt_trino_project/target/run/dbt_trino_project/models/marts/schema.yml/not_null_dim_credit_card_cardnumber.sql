select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select cardnumber
from "iceberg"."gold"."dim_credit_card"
where cardnumber is null



      
    ) dbt_internal_test