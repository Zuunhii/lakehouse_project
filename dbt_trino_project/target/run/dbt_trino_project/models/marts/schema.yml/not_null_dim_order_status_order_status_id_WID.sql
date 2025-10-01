select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select order_status_id_WID
from "iceberg"."gold"."dim_order_status"
where order_status_id_WID is null



      
    ) dbt_internal_test