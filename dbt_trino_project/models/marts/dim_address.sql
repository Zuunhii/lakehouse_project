{{ config(materialized='table') }}

with stg_address as (
  select * from {{ source('silver_src', 'silver_person_address') }}
),
stg_stateprovince as (
  select * from {{ source('silver_src', 'silver_person_stateprovince') }}
),
stg_countryregion as (
  select * from {{ source('silver_src', 'silver_person_countryregion') }}
)

select
  {{ dbt_utils.generate_surrogate_key(['a.addressid']) }} as address_id_WID,
  a.addressid as INTEGRATION_ID,

  -- Thuộc tính mô tả
  a.addressline1,
  a.addressline2,
  a.city,
  a.postalcode,

  -- State/Province
  cast(a.stateprovinceid as bigint) as stateprovinceid,
  sp.stateprovincecode,
  sp.name as stateprovince_name,

  -- Country/Region
  sp.countryregioncode,
  cr.name as country_name,

  -- Phụ trợ
  a.spatiallocation,
  a.rowguid,
  a.modifieddate

from stg_address a
left join stg_stateprovince sp
  on cast(a.stateprovinceid as bigint) = sp.stateprovinceid
left join stg_countryregion cr
  on sp.countryregioncode = cr.countryregioncode
