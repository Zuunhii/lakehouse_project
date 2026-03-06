



select
    1
from "iceberg"."gold"."fact_sales"

where not(total_due >= 0)

