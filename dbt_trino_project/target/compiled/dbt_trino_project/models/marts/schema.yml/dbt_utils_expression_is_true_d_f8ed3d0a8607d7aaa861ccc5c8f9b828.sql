



select
    1
from "iceberg"."gold"."dim_credit_card"

where not(cardnumber regexp_like(cardnumber, '^[0-9]{12,19}$'))

