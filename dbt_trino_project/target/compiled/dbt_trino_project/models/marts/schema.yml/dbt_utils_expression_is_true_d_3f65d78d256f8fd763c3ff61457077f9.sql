



select
    1
from "iceberg"."gold"."dim_credit_card"

where not(expmonth expmonth between 1 and 12)

