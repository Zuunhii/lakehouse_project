
    
    

select
    address_id_WID as unique_field,
    count(*) as n_records

from "iceberg"."gold"."dim_address"
where address_id_WID is not null
group by address_id_WID
having count(*) > 1


