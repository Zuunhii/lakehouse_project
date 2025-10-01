select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select cardtype
from "iceberg"."gold"."dim_credit_card"
where cardtype is null



      
    ) dbt_internal_test