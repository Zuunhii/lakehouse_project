{{ config(materialized='table') }}

select
    *
from {{ source('bronze_src', 'bronze_production_productsubcategory') }}