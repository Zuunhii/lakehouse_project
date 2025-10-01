-- models/marts/dim_customer.sql
{{ config(materialized='table') }}

with stg_customer as (
  select
      customerid,
      try_cast(personid as bigint) as personid,
      try_cast(storeid  as bigint) as storeid
  from {{ source('silver_src', 'silver_sales_customer') }}
),

stg_person as (
  select
      businessentityid,
      -- build fullname, gọn và sạch khoảng trắng
      trim(
        regexp_replace(
          concat(
            coalesce(firstname, ''),
            ' ',
            coalesce(middlename, ''),
            ' ',
            coalesce(lastname, '')
          ),
          '\\s+',
          ' '
        )
      ) as fullname
  from {{ source('silver_src', 'silver_person_person') }}
),

stg_store as (
  select
      businessentityid as storebusinessentityid,
      name as storename
  from {{ source('silver_src', 'silver_sales_store') }}
)

select
  -- SKey ổn định theo customerid
  {{ dbt_utils.generate_surrogate_key(['stg_customer.customerid']) }} as customer_id_WID,

  -- Business key gốc
  cast(stg_customer.customerid as bigint) as INTEGRATION_ID,

  -- Person
  stg_person.businessentityid,
  stg_person.fullname,

  -- Store
  stg_store.storebusinessentityid,
  stg_store.storename

from stg_customer
left join stg_person
  on stg_customer.personid = stg_person.businessentityid
left join stg_store
  on stg_customer.storeid = stg_store.storebusinessentityid

