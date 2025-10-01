
  
    

    create table "iceberg"."gold"."dim_order_status__dbt_tmp"
      
      
    as (
      

-- Lấy các trạng thái đơn hàng duy nhất từ SalesOrderHeader
with src as (
  select distinct status as order_status
  from "iceberg"."silver"."silver_sales_salesorderheader"
)

select
  -- Surrogate key ổn định theo status
  lower(to_hex(md5(to_utf8(cast(coalesce(cast(order_status as varchar), '_dbt_utils_surrogate_key_null_') as varchar))))) as order_status_id_WID,

  -- Business key
  order_status as INTEGRATION_ID,

  -- Tên hiển thị (AdventureWorks conventions)
  case order_status
    when 1 then 'In Process'
    when 2 then 'Approved'
    when 3 then 'Backordered'
    when 4 then 'Rejected'
    when 5 then 'Shipped'
    when 6 then 'Cancelled'
    else 'Unknown'
  end as order_status_name

from src
    );

  