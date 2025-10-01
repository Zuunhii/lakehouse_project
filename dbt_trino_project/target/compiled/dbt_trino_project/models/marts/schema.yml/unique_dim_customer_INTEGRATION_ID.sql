
    
    

select
    INTEGRATION_ID as unique_field,
    count(*) as n_records

from "iceberg"."gold"."dim_customer"
where INTEGRATION_ID is not null
group by INTEGRATION_ID
having count(*) > 1


