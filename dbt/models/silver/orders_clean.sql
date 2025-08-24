{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    on_schema_change='sync_all_columns'
) }}

with src as (
  select
    cast(order_id as varchar)       as order_id,
    cast(customer_id as varchar)    as customer_id,
    try_cast(order_ts as timestamp) as order_ts,
    nullif(trim(status), '')        as status,
    try_cast(amount as double)      as amount
  from {{ source('bronze_src','orders_raw') }}
  where order_id is not null
)
select * from src

{% if is_incremental() %}
  where order_ts >= (select coalesce(max(order_ts), timestamp '1970-01-01') from {{ this }})
{% endif %}
