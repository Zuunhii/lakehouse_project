-- =========================================================
-- ONE-TIME: create Bronze schema in Hive catalog (minio)
-- =========================================================
CREATE SCHEMA IF NOT EXISTS minio.bronze
WITH (location='s3a://lakehouse/bronze/');

-- ======================
-- HumanResources
-- ======================
CREATE TABLE IF NOT EXISTS minio.bronze.humanresources_department
WITH (external_location='s3a://lakehouse/bronze/HumanResources/Department/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.humanresources_employee
WITH (external_location='s3a://lakehouse/bronze/HumanResources/Employee/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.humanresources_employeedepartmenthistory
WITH (external_location='s3a://lakehouse/bronze/HumanResources/EmployeeDepartmentHistory/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.humanresources_employeepayhistory
WITH (external_location='s3a://lakehouse/bronze/HumanResources/EmployeePayHistory/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.humanresources_jobcandidate
WITH (external_location='s3a://lakehouse/bronze/HumanResources/JobCandidate/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.humanresources_shift
WITH (external_location='s3a://lakehouse/bronze/HumanResources/Shift/', format='PARQUET');

-- ======================
-- Person
-- ======================
CREATE TABLE IF NOT EXISTS minio.bronze.person_address
WITH (external_location='s3a://lakehouse/bronze/Person/Address/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.person_addresstype
WITH (external_location='s3a://lakehouse/bronze/Person/AddressType/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.person_businessentity
WITH (external_location='s3a://lakehouse/bronze/Person/BusinessEntity/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.person_businessentityaddress
WITH (external_location='s3a://lakehouse/bronze/Person/BusinessEntityAddress/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.person_businessentitycontact
WITH (external_location='s3a://lakehouse/bronze/Person/BusinessEntityContact/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.person_contacttype
WITH (external_location='s3a://lakehouse/bronze/Person/ContactType/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.person_countryregion
WITH (external_location='s3a://lakehouse/bronze/Person/CountryRegion/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.person_emailaddress
WITH (external_location='s3a://lakehouse/bronze/Person/EmailAddress/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.person_password
WITH (external_location='s3a://lakehouse/bronze/Person/Password/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.person_person
WITH (external_location='s3a://lakehouse/bronze/Person/Person/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.person_personphone
WITH (external_location='s3a://lakehouse/bronze/Person/PersonPhone/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.person_phonenumbertype
WITH (external_location='s3a://lakehouse/bronze/Person/PhoneNumberType/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.person_stateprovince
WITH (external_location='s3a://lakehouse/bronze/Person/StateProvince/', format='PARQUET');

-- ======================
-- Production
-- ======================
CREATE TABLE IF NOT EXISTS minio.bronze.production_billofmaterials
WITH (external_location='s3a://lakehouse/bronze/Production/BillOfMaterials/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.production_culture
WITH (external_location='s3a://lakehouse/bronze/Production/Culture/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.production_document
WITH (external_location='s3a://lakehouse/bronze/Production/Document/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.production_illustration
WITH (external_location='s3a://lakehouse/bronze/Production/Illustration/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.production_location
WITH (external_location='s3a://lakehouse/bronze/Production/Location/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.production_product
WITH (external_location='s3a://lakehouse/bronze/Production/Product/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.production_productcategory
WITH (external_location='s3a://lakehouse/bronze/Production/ProductCategory/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.production_productcosthistory
WITH (external_location='s3a://lakehouse/bronze/Production/ProductCostHistory/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.production_productdescription
WITH (external_location='s3a://lakehouse/bronze/Production/ProductDescription/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.production_productdocument
WITH (external_location='s3a://lakehouse/bronze/Production/ProductDocument/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.production_productinventory
WITH (external_location='s3a://lakehouse/bronze/Production/ProductInventory/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.production_productlistpricehistory
WITH (external_location='s3a://lakehouse/bronze/Production/ProductListPriceHistory/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.production_productmodel
WITH (external_location='s3a://lakehouse/bronze/Production/ProductModel/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.production_productmodelillustration
WITH (external_location='s3a://lakehouse/bronze/Production/ProductModelIllustration/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.production_productmodelproductdescriptionculture
WITH (external_location='s3a://lakehouse/bronze/Production/ProductModelProductDescriptionCulture/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.production_productphoto
WITH (external_location='s3a://lakehouse/bronze/Production/ProductPhoto/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.production_productproductphoto
WITH (external_location='s3a://lakehouse/bronze/Production/ProductProductPhoto/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.production_productreview
WITH (external_location='s3a://lakehouse/bronze/Production/ProductReview/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.production_productsubcategory
WITH (external_location='s3a://lakehouse/bronze/Production/ProductSubcategory/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.production_scrapreason
WITH (external_location='s3a://lakehouse/bronze/Production/ScrapReason/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.production_transactionhistory
WITH (external_location='s3a://lakehouse/bronze/Production/TransactionHistory/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.production_transactionhistoryarchive
WITH (external_location='s3a://lakehouse/bronze/Production/TransactionHistoryArchive/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.production_unitmeasure
WITH (external_location='s3a://lakehouse/bronze/Production/UnitMeasure/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.production_workorder
WITH (external_location='s3a://lakehouse/bronze/Production/WorkOrder/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.production_workorderrouting
WITH (external_location='s3a://lakehouse/bronze/Production/WorkOrderRouting/', format='PARQUET');

-- ======================
-- Purchasing
-- ======================
CREATE TABLE IF NOT EXISTS minio.bronze.purchasing_productvendor
WITH (external_location='s3a://lakehouse/bronze/Purchasing/ProductVendor/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.purchasing_purchaseorderdetail
WITH (external_location='s3a://lakehouse/bronze/Purchasing/PurchaseOrderDetail/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.purchasing_purchaseorderheader
WITH (external_location='s3a://lakehouse/bronze/Purchasing/PurchaseOrderHeader/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.purchasing_shipmethod
WITH (external_location='s3a://lakehouse/bronze/Purchasing/ShipMethod/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.purchasing_vendor
WITH (external_location='s3a://lakehouse/bronze/Purchasing/Vendor/', format='PARQUET');

-- ======================
-- Sales
-- ======================
CREATE TABLE IF NOT EXISTS minio.bronze.sales_countryregioncurrency
WITH (external_location='s3a://lakehouse/bronze/Sales/CountryRegionCurrency/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.sales_creditcard
WITH (external_location='s3a://lakehouse/bronze/Sales/CreditCard/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.sales_currency
WITH (external_location='s3a://lakehouse/bronze/Sales/Currency/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.sales_currencyrate
WITH (external_location='s3a://lakehouse/bronze/Sales/CurrencyRate/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.sales_customer
WITH (external_location='s3a://lakehouse/bronze/Sales/Customer/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.sales_personcreditcard
WITH (external_location='s3a://lakehouse/bronze/Sales/PersonCreditCard/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.sales_salesorderdetail
WITH (external_location='s3a://lakehouse/bronze/Sales/SalesOrderDetail/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.sales_salesorderheader
WITH (external_location='s3a://lakehouse/bronze/Sales/SalesOrderHeader/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.sales_salesorderheadersalesreason
WITH (external_location='s3a://lakehouse/bronze/Sales/SalesOrderHeaderSalesReason/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.sales_salesperson
WITH (external_location='s3a://lakehouse/bronze/Sales/SalesPerson/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.sales_salespersonquotahistory
WITH (external_location='s3a://lakehouse/bronze/Sales/SalesPersonQuotaHistory/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.sales_salesreason
WITH (external_location='s3a://lakehouse/bronze/Sales/SalesReason/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.sales_salestaxrate
WITH (external_location='s3a://lakehouse/bronze/Sales/SalesTaxRate/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.sales_salesterritory
WITH (external_location='s3a://lakehouse/bronze/Sales/SalesTerritory/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.sales_salesterritoryhistory
WITH (external_location='s3a://lakehouse/bronze/Sales/SalesTerritoryHistory/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.sales_shoppingcartitem
WITH (external_location='s3a://lakehouse/bronze/Sales/ShoppingCartItem/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.sales_specialoffer
WITH (external_location='s3a://lakehouse/bronze/Sales/SpecialOffer/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.sales_specialofferproduct
WITH (external_location='s3a://lakehouse/bronze/Sales/SpecialOfferProduct/', format='PARQUET');

CREATE TABLE IF NOT EXISTS minio.bronze.sales_store
WITH (external_location='s3a://lakehouse/bronze/Sales/Store/', format='PARQUET');
