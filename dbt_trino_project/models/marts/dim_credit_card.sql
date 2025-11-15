-- models/marts/dim_credit_card.sql
{{ config(materialized='table') }}

with stg_salesorderheader as (
    select distinct
        creditcardid
    from {{ source('silver_src', 'silver_sales_salesorderheader') }}
    where creditcardid is not null
),

stg_creditcard as (
    select
        creditcardid,
        cardtype
    from {{ source('silver_src', 'silver_sales_creditcard') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['stg_salesorderheader.creditcardid']) }} as credit_card_id_WID,
    cast(stg_salesorderheader.creditcardid as bigint) as INTEGRATION_ID,
    stg_creditcard.cardtype 
from stg_salesorderheader
left join stg_creditcard
  on stg_salesorderheader.creditcardid = stg_creditcard.creditcardid
