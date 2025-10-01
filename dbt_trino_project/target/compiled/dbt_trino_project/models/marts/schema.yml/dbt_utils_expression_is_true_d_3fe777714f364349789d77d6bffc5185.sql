



select
    1
from "iceberg"."gold"."dim_credit_card"

where not(last4 length() = 4 and regexp_like(, '^[0-9]{4}$'))

