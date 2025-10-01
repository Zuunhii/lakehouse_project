select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    INTEGRATION_ID as unique_field,
    count(*) as n_records

from "iceberg"."gold"."dim_order_status"
where INTEGRATION_ID is not null
group by INTEGRATION_ID
having count(*) > 1



      
    ) dbt_internal_test