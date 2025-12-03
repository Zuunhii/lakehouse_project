{{ config(
    materialized = 'table',
    path = 'iceberg/person/address/') 
}}

select
    *
from {{ source('bronze_src', 'bronze_person_address') }}
