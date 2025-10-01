



select
    1
from "iceberg"."gold"."dim_credit_card"

where not(cardnumber regexp_like(, '^[0-9]{12,19}$'))

