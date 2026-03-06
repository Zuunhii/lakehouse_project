
    
    

select
    sales_id_wid as unique_field,
    count(*) as n_records

from "iceberg"."gold"."fact_sales_LIVE"
where sales_id_wid is not null
group by sales_id_wid
having count(*) > 1


