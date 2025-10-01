

with stg_product as (
  select * from "iceberg"."silver"."silver_production_product"
),
stg_product_subcateg as (
  select * from "iceberg"."silver"."silver_production_productsubcategory"
),
stg_product_category as (
  select * from "iceberg"."silver"."silver_production_productcategory"
)

select
  lower(to_hex(md5(to_utf8(cast(coalesce(cast(p.productid as varchar), '_dbt_utils_surrogate_key_null_') as varchar))))) as product_id_WID,
  p.productid as INTEGRATION_ID,
  p.name as product_name,
  p.productnumber,
  p.makeflag,
  p.finishedgoodsflag,
  p.color,
  p.productline,
  p.class,
  p.style,
  p.size,
  p.sizeunitmeasurecode,
  p.weightunitmeasurecode,
  p.weight,
  p.safetystocklevel,
  p.reorderpoint,
  p.standardcost,
  p.listprice,
  p.daystomanufacture,
  p.sellstartdate,
  p.sellenddate,
  p.discontinueddate,
  cast(p.productsubcategoryid as bigint) as productsubcategoryid,
  psc.name as product_subcategory_name,
  pc.productcategoryid,
  pc.name as product_category_name
from stg_product p
left join stg_product_subcateg psc
  on cast(p.productsubcategoryid as bigint) = psc.productsubcategoryid
left join stg_product_category pc
  on psc.productcategoryid = pc.productcategoryid