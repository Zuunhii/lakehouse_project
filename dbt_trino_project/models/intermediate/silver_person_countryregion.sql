{{ config(materialized='table') }}

select
    *
from {{ source('bronze_src', 'bronze_person_countryregion') }}