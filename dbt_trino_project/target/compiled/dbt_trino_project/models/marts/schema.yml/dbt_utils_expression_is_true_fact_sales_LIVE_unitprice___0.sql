



select
    1
from "iceberg"."gold"."fact_sales_LIVE"

where not(unitprice >= 0)

