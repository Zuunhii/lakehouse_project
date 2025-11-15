
  
    

    create table "iceberg"."gold"."dim_address__dbt_tmp"
      
      
    as (
      

with stg_address as (
  select * from "iceberg"."silver"."silver_person_address"
),
stg_stateprovince as (
  select * from "iceberg"."silver"."silver_person_stateprovince"
),
stg_countryregion as (
  select * from "iceberg"."silver"."silver_person_countryregion"
)

select
  lower(to_hex(md5(to_utf8(cast(coalesce(cast(a.addressid as varchar), '_dbt_utils_surrogate_key_null_') as varchar))))) as address_id_WID,
  a.addressid as INTEGRATION_ID,

  -- Thuộc tính mô tả
  a.addressline1 AS address_line_1,
  a.addressline2 AS address_line_2,
  a.city,
  a.postalcode AS postal_code,

  -- State/Province
  cast(a.stateprovinceid as bigint) as state_province_id,
  sp.stateprovincecode AS state_province_code,
  sp.name as state_province_name,

  -- Country/Region
  sp.countryregioncode AS country_region_code,
  cr.name as country_name,

  -- Phụ trợ
  -- a.spatiallocation,
  -- a.rowguid,
  a.modifieddate

from stg_address a
left join stg_stateprovince sp
  on cast(a.stateprovinceid as bigint) = sp.stateprovinceid
left join stg_countryregion cr
  on sp.countryregioncode = cr.countryregioncode
    );

  