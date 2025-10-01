select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select credit_card_id_WID
from "iceberg"."gold"."dim_credit_card"
where credit_card_id_WID is null



      
    ) dbt_internal_test