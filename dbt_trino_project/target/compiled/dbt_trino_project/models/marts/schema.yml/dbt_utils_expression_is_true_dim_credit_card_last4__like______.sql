



select
    1
from "iceberg"."gold"."dim_credit_card"

where not(last4 like '____')

