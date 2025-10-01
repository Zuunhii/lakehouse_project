
    
    

select
    credit_card_id_WID as unique_field,
    count(*) as n_records

from "iceberg"."gold"."dim_credit_card"
where credit_card_id_WID is not null
group by credit_card_id_WID
having count(*) > 1


