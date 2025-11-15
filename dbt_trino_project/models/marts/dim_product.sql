{{ config(materialized='table') }}

with stg_product as (
  select * from {{ source('silver_src', 'silver_production_product') }}
),
stg_product_subcateg as (
  select * from {{ source('silver_src', 'silver_production_productsubcategory') }}
),
stg_product_category as (
  select * from {{ source('silver_src', 'silver_production_productcategory') }}
)

select
  {{ dbt_utils.generate_surrogate_key(['p.productid']) }} as product_id_WID,
  p.productid as INTEGRATION_ID,
  p.name as product_name,
  p.productnumber,
  --p.makeflag,
  --p.finishedgoodsflag,
  p.color,
  --p.productline,
  p.class,
  --p.style,
  p.size,
  --p.sizeunitmeasurecode,
  --p.weightunitmeasurecode,
  -- p.weight,
  -- p.safetystocklevel,
  -- p.reorderpoint,
  -- p.standardcost,
  -- p.listprice,
  -- p.daystomanufacture,
  -- p.sellstartdate,
  -- p.sellenddate,
  -- p.discontinueddate,
  cast(p.productsubcategoryid as bigint) as productsubcategoryid,
  psc.name as product_subcategory_name,
  pc.productcategoryid,
  pc.name as product_category_name
from stg_product p
left join stg_product_subcateg psc
  on cast(p.productsubcategoryid as bigint) = psc.productsubcategoryid
left join stg_product_category pc
  on psc.productcategoryid = pc.productcategoryid
