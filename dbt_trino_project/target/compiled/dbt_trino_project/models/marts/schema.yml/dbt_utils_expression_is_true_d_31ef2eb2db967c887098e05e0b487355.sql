



select
    1
from "iceberg"."gold"."dim_credit_card"

where not(expyear between 2000 and 2100)

