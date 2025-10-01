
  
    

    create table "iceberg"."gold"."dim_credit_card__dbt_tmp"
      
      
    as (
      -- models/marts/dim_credit_card.sql


with stg_salesorderheader as (
    select distinct
        creditcardid
    from "iceberg"."silver"."silver_sales_salesorderheader"
    where creditcardid is not null
),

stg_creditcard as (
    select
        creditcardid,
        cardtype
    from "iceberg"."silver"."silver_sales_creditcard"
)

select
    lower(to_hex(md5(to_utf8(cast(coalesce(cast(stg_salesorderheader.creditcardid as varchar), '_dbt_utils_surrogate_key_null_') as varchar))))) as credit_card_id_WID,
    cast(stg_salesorderheader.creditcardid as bigint) as INTEGRATION_ID,
    stg_creditcard.cardtype
from stg_salesorderheader
left join stg_creditcard
  on stg_salesorderheader.creditcardid = stg_creditcard.creditcardid
    );

  