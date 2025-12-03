{{ config(materialized='table') }}

select
    *
from {{ source('bronze_src', 'bronze_sales_salesorderheadersalesreason') }}
