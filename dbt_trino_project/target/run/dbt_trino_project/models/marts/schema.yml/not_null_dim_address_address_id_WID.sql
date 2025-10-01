select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select address_id_WID
from "iceberg"."gold"."dim_address"
where address_id_WID is null



      
    ) dbt_internal_test