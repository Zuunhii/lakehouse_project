-- -- models/marts/dim_customer.sql
-- 

-- with stg_customer as (
--   select
--       customerid,
--       try_cast(personid as bigint) as personid,
--       try_cast(storeid  as bigint) as storeid
--   from "iceberg"."silver"."silver_sales_customer"
-- ),

-- stg_person as (
--   select
--       businessentityid,
--       -- build fullname, gọn và sạch khoảng trắng
--       trim(
--         regexp_replace(
--           concat(
--             coalesce(firstname, ''),
--             ' ',
--             coalesce(middlename, ''),
--             ' ',
--             coalesce(lastname, '')
--           ),
--           '\\s+',
--           ' '
--         )
--       ) as fullname
--   from "iceberg"."silver"."silver_person_person"
-- ),

-- stg_store as (
--   select
--       businessentityid as storebusinessentityid,
--       name as storename
--   from "iceberg"."silver"."silver_sales_store"
-- )

-- select
--   -- SKey ổn định theo customerid
--   lower(to_hex(md5(to_utf8(cast(coalesce(cast(stg_customer.customerid as varchar), '_dbt_utils_surrogate_key_null_') as varchar))))) as customer_id_WID,

--   -- Business key gốc
--   cast(stg_customer.customerid as bigint) as INTEGRATION_ID,

--   -- Person
--   stg_person.businessentityid AS person_business_entity_id,
--   stg_person.fullname,

--   -- Store
--   stg_store.storebusinessentityid AS store_business_entity_id,
--   stg_store.storename AS store_name

-- from stg_customer
-- left join stg_person
--   on stg_customer.personid = stg_person.businessentityid
-- left join stg_store
--   on stg_customer.storeid = stg_store.storebusinessentityid







with stg_customer as (
    select
        customerid,
        try_cast(personid as bigint) as personid,
        -- Cột quan trọng: Lấy AccountNumber để dùng làm CustomerAlternateKey
        accountnumber 
    from "iceberg"."silver"."silver_sales_customer"
),

person_raw as (
    select
        businessentityid, persontype, namestyle, title, firstname, middlename, lastname, suffix, 
        emailpromotion, additionalcontactinfo, demographics, rowguid, modifieddate
    from "iceberg"."silver"."silver_person_person"
),

email_raw as (
    select
        businessentityid, emailaddressid, emailaddress, modifieddate
    from "iceberg"."silver"."silver_person_emailaddress"
),

phone_raw as (
    select
        businessentityid, phonenumber, phonenumbertypeid, modifieddate
    from "iceberg"."silver"."silver_person_personphone"
),

bea_raw as (
    select
        businessentityid, addressid, addresstypeid, rowguid, modifieddate
    from "iceberg"."silver"."silver_person_businessentityaddress"
),

address_raw as (
    select
        addressid, addressline1, addressline2, city, stateprovinceid, postalcode, spatiallocation, 
        rowguid, modifieddate
    from "iceberg"."silver"."silver_person_address"
),

stateprovince_raw as (
    select
        stateprovinceid, stateprovincecode, countryregioncode, isonlystateprovinceflag, 
        name as stateprovince_name, territoryid, rowguid, modifieddate
    from "iceberg"."silver"."silver_person_stateprovince"
),

countryregion_raw as (
    select
        countryregioncode, name as countryregion_name, modifieddate
    from "iceberg"."silver"."silver_person_countryregion"
),



person_name_clean as (
    select
        businessentityid, namestyle,
        -- NULL nếu chuỗi toàn space / rỗng
        nullif(trim(title), '') as title,
        nullif(trim(firstname), '') as firstname,
        nullif(trim(middlename), '') as middlename,
        nullif(trim(lastname), '') as lastname,
        nullif(trim(suffix), '') as suffix
    from person_raw
),

/* 2.1 Clean Email (Dedup, Trim, Lowercase, Validate Format) */
email_dedup as (
    select
        businessentityid, emailaddressid, emailaddress, modifieddate,
        row_number() over (
            partition by businessentityid
            order by modifieddate desc, emailaddressid desc
        ) as rn
    from email_raw
),

email_clean as (
    select
        businessentityid,
        case
            when emailaddress is null then null
            when trim(emailaddress) = '' then null
            when not regexp_like(
                lower(trim(emailaddress)),
                '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
            ) then null
            else lower(trim(emailaddress))
        end as emailaddress
    from email_dedup
    where rn = 1
),

/* 2.2 Clean Phone (Dedup, Normalize digits, Validate Length) */
phone_dedup as (
    select
        businessentityid, phonenumber, modifieddate,
        row_number() over (
            partition by businessentityid
            order by modifieddate desc
        ) as rn
    from phone_raw
),

phone_clean as (
    select
        businessentityid,
        case
            when phonenumber is null then null
            else
                case
                    when length(regexp_replace(phonenumber, '[^0-9]', '')) < 7
                        then null
                    else regexp_replace(phonenumber, '[^0-9]', '')
                end
        end as phonenumber
    from phone_dedup
    where rn = 1
),



/* 3.1 Dedup address per person */
bea_dedup as (
    select
        businessentityid, addressid, addresstypeid, modifieddate,
        row_number() over (
            partition by businessentityid
            order by modifieddate desc, addressid desc
        ) as rn
    from bea_raw
),

bea_one as (
    select
        businessentityid, addressid, addresstypeid
    from bea_dedup
    where rn = 1
),

address_clean as (
    select
        addressid,
        nullif(trim(addressline1), '') as addressline1,
        nullif(trim(addressline2), '') as addressline2,
        nullif(trim(city), '') as city,
        stateprovinceid,
        nullif(trim(postalcode), '') as postalcode
    from address_raw
),

stateprovince_clean as (
    select
        stateprovinceid,
        nullif(trim(stateprovince_name), '') as stateprovince_name,
        countryregioncode
    from stateprovince_raw
),

countryregion_clean as (
    select
        countryregioncode,
        nullif(trim(countryregion_name), '') as countryregion_name
    from countryregion_raw
),

person_geography as (
    select
        bea.businessentityid,
        addr.addressline1,
        addr.addressline2,
        addr.city,
        sp.stateprovince_name,
        cr.countryregion_name,

        lower(to_hex(md5(to_utf8(cast(coalesce(cast(addr.addressline1 as varchar), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(addr.city as varchar), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(sp.stateprovince_name as varchar), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(cr.countryregion_name as varchar), '_dbt_utils_surrogate_key_null_') as varchar))))) as geographykey

    from bea_one bea
    left join address_clean addr
        on bea.addressid = addr.addressid
    left join stateprovince_clean sp
        on addr.stateprovinceid = sp.stateprovinceid
    left join countryregion_clean cr
        on sp.countryregioncode = cr.countryregioncode
)



select
    -- Surrogate key cho customer
    lower(to_hex(md5(to_utf8(cast(coalesce(cast(c.customerid as varchar), '_dbt_utils_surrogate_key_null_') as varchar))))) as customer_id_WID,
    -- Business key gốc
    cast(c.customerid as bigint) as INTEGRATION_ID,
    -- GeographyKey
    geo.geographykey as GeographyKey,

    -- CustomerAlternateKey
    c.accountnumber as CustomerAlternateKey, 

    -- Tên (đã clean)
    p.title as Title,
    
    -- 🔥 Đã sửa: Bọc NULLIF(TRIM(...), '') để chuyển chuỗi rỗng thành NULL, sau đó dùng COALESCE để gán tên mặc định
    COALESCE(
        NULLIF(
            TRIM(
                COALESCE(p.firstname, '') || 
                CASE WHEN TRIM(COALESCE(p.middlename, '')) != '' THEN ' ' || p.middlename ELSE '' END || 
                CASE WHEN TRIM(COALESCE(p.lastname, '')) != '' THEN ' ' || p.lastname ELSE '' END
            ),
            ''
        ),
        'Unknown Customer' -- Gán tên mặc định nếu không có tên
    ) AS FullName,

    -- LOẠI BỎ các cột tên riêng lẻ (FirstName, MiddleName, LastName)
    
    p.namestyle as NameStyle,
    p.suffix as Suffix,

    -- Email & Phone (đã clean)
    em.emailaddress as EmailAddress,
    ph.phonenumber as Phone,

    -- Địa chỉ (đã clean)
    geo.addressline1 as AddressLine1,
    geo.addressline2 as AddressLine2,

    -- Extra helper columns
    geo.city as City,
    geo.stateprovince_name as StateProvinceName,
    geo.countryregion_name as CountryRegionName,
    c.personid as PersonBusinessEntityId

from stg_customer c
left join person_name_clean p
    on c.personid = p.businessentityid
left join email_clean em
    on p.businessentityid = em.businessentityid
left join phone_clean ph
    on p.businessentityid = ph.businessentityid
left join person_geography geo
    on p.businessentityid = geo.businessentityid