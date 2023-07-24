{{ config(
    schema='BLACKPURL_PRODUCTION',
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['STOCK_NUMBER', 'ORDER_NUMBER'],
    pre_hook=[
        "{{ delete_duplicates_deal_units('blackpurl_production', 'deal_units_stage', 'temp_deal_units_stage_table') }}",
        "{{ delete_duplicates_deal_units('blackpurl_production_blackpurl_production', 'deal_units_prod', 'temp_deal_units_prod_table') }}",
    ],

    post_hook="TRUNCATE TABLE {{ source('blackpurl_production', 'deal_units_stage') }};"

) }}


WITH stage AS (
    SELECT 
    _FILE,
    _LINE,
    _MODIFIED,
    TYPE,
    STOCK_NUMBER,
    MAKE,
    MODEL,
    SUBMODEL,
    YEAR,
    STATUS,
    ORDER_NUMBER,
    TRY_CAST(REPLACE(REPLACE(TOTAL_PRICE, '$', ''), ',', '') AS FLOAT) AS TOTAL_PRICE,
    DEAL_TYPE,
    CUSTOMER,
    COALESCE(TRY_TO_DATE(DATE_SOLD, 'MM/DD/YYYY'), 
                TRY_TO_DATE(DATE_SOLD, 'M/D/YYYY'), 
                TRY_TO_DATE(DATE_SOLD, 'MM-DD-YYYY'), 
                TRY_TO_DATE(DATE_SOLD, 'M-D-YYYY'), 
                TRY_TO_DATE(DATE_SOLD, 'YYYY-MM-DD'),
                TRY_TO_DATE(DATE_SOLD, 'MM/DD/YYYY HH24:MI'),
                TRY_TO_DATE(DATE_SOLD, 'MM/DD/YYYY HH12:MI AM')) 
        AS formatted_date,
    TRY_CAST(REPLACE(REPLACE(BASE_PRICE, '$', ''), ',', '') AS FLOAT) AS BASE_PRICE,
    TRY_CAST(REPLACE(REPLACE(FACTORY_OPTIONS, '$', ''), ',', '') AS FLOAT) AS FACTORY_OPTIONS,
    TRY_CAST(REPLACE(REPLACE(DEALER_INSTALLED, '$', ''), ',', '') AS FLOAT) AS DEALER_INSTALLED,
    TRY_CAST(REPLACE(REPLACE(PART_OPTIONS, '$', ''), ',', '') AS FLOAT) AS PART_OPTIONS,
    TRY_CAST(REPLACE(REPLACE(LABOR_OPTIONS, '$', ''), ',', '') AS FLOAT) AS LABOR_OPTIONS,
    TRY_CAST(REPLACE(REPLACE(SUBLET_OPTIONS, '$', ''), ',', '') AS FLOAT) AS SUBLET_OPTIONS,
    TRY_CAST(REPLACE(REPLACE(FEE_OPTIONS, '$', ''), ',', '') AS FLOAT) AS FEE_OPTIONS,
    TRY_CAST(REPLACE(REPLACE(WARRANTY_OPTIONS, '$', ''), ',', '') AS FLOAT) AS WARRANTY_OPTIONS,
    TRY_CAST(REPLACE(REPLACE(OTHER_PRODUCTS, '$', ''), ',', '') AS FLOAT) AS OTHER_PRODUCTS,
    TRY_CAST(REPLACE(REPLACE(STAMP_DUTY, '$', ''), ',', '') AS FLOAT) AS STAMP_DUTY,
    VIN,
    NEW_UNIT,
    CATEGORY,
    STOCKED_IN,
    AGE,
    TAGS,
    MILEAGE,
    MILEAGE_TYPE,
    EXT_COLOR,
    FIRST_NAME,
    LAST_NAME,
    ADDRESS,
    CITY,
    STATE,
    POSTAL,
    PHONE,
    MOBILE,
    EMAIL,
    OTHER_EMAIL,
    SALESPERSON,
    GVWR,
    DESCRIPTION,
    _FIVETRAN_SYNCED
    FROM {{ source('blackpurl_production', 'deal_units_stage') }}
    where formatted_date is not null
),

prod AS ( 
    SELECT 
    _FILE,
    _LINE,
    _MODIFIED,
    TYPE,
    STOCK_NUMBER,
    MAKE,
    MODEL,
    SUBMODEL,
    YEAR,
    STATUS,
    ORDER_NUMBER,
    TRY_CAST(REPLACE(REPLACE(TOTAL_PRICE, '$', ''), ',', '') AS FLOAT) AS TOTAL_PRICE,
    DEAL_TYPE,
    CUSTOMER,
    COALESCE(TRY_TO_DATE(DATE_SOLD, 'MM/DD/YYYY'), 
                TRY_TO_DATE(DATE_SOLD, 'M/D/YYYY'), 
                TRY_TO_DATE(DATE_SOLD, 'MM-DD-YYYY'), 
                TRY_TO_DATE(DATE_SOLD, 'M-D-YYYY'), 
                TRY_TO_DATE(DATE_SOLD, 'YYYY-MM-DD'),
                TRY_TO_DATE(DATE_SOLD, 'MM/DD/YYYY HH24:MI'),
                TRY_TO_DATE(DATE_SOLD, 'MM/DD/YYYY HH12:MI AM')) 
        AS formatted_date,
    TRY_CAST(REPLACE(REPLACE(BASE_PRICE, '$', ''), ',', '') AS FLOAT) AS BASE_PRICE,
    TRY_CAST(REPLACE(REPLACE(FACTORY_OPTIONS, '$', ''), ',', '') AS FLOAT) AS FACTORY_OPTIONS,
    TRY_CAST(REPLACE(REPLACE(DEALER_INSTALLED, '$', ''), ',', '') AS FLOAT) AS DEALER_INSTALLED,
    TRY_CAST(REPLACE(REPLACE(PART_OPTIONS, '$', ''), ',', '') AS FLOAT) AS PART_OPTIONS,
    TRY_CAST(REPLACE(REPLACE(LABOR_OPTIONS, '$', ''), ',', '') AS FLOAT) AS LABOR_OPTIONS,
    TRY_CAST(REPLACE(REPLACE(SUBLET_OPTIONS, '$', ''), ',', '') AS FLOAT) AS SUBLET_OPTIONS,
    TRY_CAST(REPLACE(REPLACE(FEE_OPTIONS, '$', ''), ',', '') AS FLOAT) AS FEE_OPTIONS,
    TRY_CAST(REPLACE(REPLACE(WARRANTY_OPTIONS, '$', ''), ',', '') AS FLOAT) AS WARRANTY_OPTIONS,
    TRY_CAST(REPLACE(REPLACE(OTHER_PRODUCTS, '$', ''), ',', '') AS FLOAT) AS OTHER_PRODUCTS,
    TRY_CAST(REPLACE(REPLACE(STAMP_DUTY, '$', ''), ',', '') AS FLOAT) AS STAMP_DUTY,
    VIN,
    NEW_UNIT,
    CATEGORY,
    STOCKED_IN,
    AGE,
    TAGS,
    MILEAGE,
    MILEAGE_TYPE,
    EXT_COLOR,
    FIRST_NAME,
    LAST_NAME,
    ADDRESS,
    CITY,
    STATE,
    POSTAL,
    PHONE,
    MOBILE,
    EMAIL,
    OTHER_EMAIL,
    SALESPERSON,
    GVWR,
    DESCRIPTION,
    _FIVETRAN_SYNCED
    FROM {{ source('blackpurl_production_blackpurl_production', 'deal_units_prod') }}
    where formatted_date is not null
),

matched_records AS (
    SELECT
        stage.*
    FROM stage
    JOIN prod
        ON stage.STOCK_NUMBER = prod.STOCK_NUMBER
        AND stage.ORDER_NUMBER = prod.ORDER_NUMBER
),

updates AS (
    SELECT
    matched_records._FILE,
    matched_records._LINE,
    matched_records._MODIFIED,
    matched_records.TYPE,
    matched_records.STOCK_NUMBER,
    matched_records.MAKE,
    matched_records.MODEL,
    matched_records.SUBMODEL,
    matched_records.YEAR,
    matched_records.STATUS,
    matched_records.ORDER_NUMBER,
    matched_records.TOTAL_PRICE,
    matched_records.DEAL_TYPE,
    matched_records.CUSTOMER,
    matched_records.formatted_date as DATE_SOLD,
    matched_records.BASE_PRICE,
    matched_records.FACTORY_OPTIONS,
    matched_records.DEALER_INSTALLED,
    matched_records.PART_OPTIONS,
    matched_records.LABOR_OPTIONS,
    matched_records.SUBLET_OPTIONS,
    matched_records.FEE_OPTIONS,
    matched_records.WARRANTY_OPTIONS,
    matched_records.OTHER_PRODUCTS,
    matched_records.STAMP_DUTY,
    matched_records.VIN,
    matched_records.NEW_UNIT,
    matched_records.CATEGORY,
    matched_records.STOCKED_IN,
    matched_records.AGE,
    matched_records.TAGS,
    matched_records.MILEAGE,
    matched_records.MILEAGE_TYPE,
    matched_records.EXT_COLOR,
    matched_records.FIRST_NAME,
    matched_records.LAST_NAME,
    matched_records.ADDRESS,
    matched_records.CITY,
    matched_records.STATE,
    matched_records.POSTAL,
    matched_records.PHONE,
    matched_records.MOBILE,
    matched_records.EMAIL,
    matched_records.OTHER_EMAIL,
    matched_records.SALESPERSON,
    matched_records.GVWR,
    matched_records.DESCRIPTION,
    matched_records._FIVETRAN_SYNCED
    FROM matched_records),

non_matched_records AS (
    SELECT
        stage.*
    FROM stage
    LEFT JOIN prod
        ON stage.STOCK_NUMBER = prod.STOCK_NUMBER
        AND stage.ORDER_NUMBER = prod.ORDER_NUMBER
    WHERE prod.STOCK_NUMBER IS NULL AND prod.ORDER_NUMBER IS NULL
),

inserts AS (
    SELECT
    non_matched_records._FILE,
    non_matched_records._LINE,
    non_matched_records._MODIFIED,
    non_matched_records.TYPE,
    non_matched_records.STOCK_NUMBER,
    non_matched_records.MAKE,
    non_matched_records.MODEL,
    non_matched_records.SUBMODEL,
    non_matched_records.YEAR,
    non_matched_records.STATUS,
    non_matched_records.ORDER_NUMBER,
    non_matched_records.TOTAL_PRICE,
    non_matched_records.DEAL_TYPE,
    non_matched_records.CUSTOMER,
    non_matched_records.formatted_date as DATE_SOLD,
    non_matched_records.BASE_PRICE,
    non_matched_records.FACTORY_OPTIONS,
    non_matched_records.DEALER_INSTALLED,
    non_matched_records.PART_OPTIONS,
    non_matched_records.LABOR_OPTIONS,
    non_matched_records.SUBLET_OPTIONS,
    non_matched_records.FEE_OPTIONS,
    non_matched_records.WARRANTY_OPTIONS,
    non_matched_records.OTHER_PRODUCTS,
    non_matched_records.STAMP_DUTY,
    non_matched_records.VIN,
    non_matched_records.NEW_UNIT,
    non_matched_records.CATEGORY,
    non_matched_records.STOCKED_IN,
    non_matched_records.AGE,
    non_matched_records.TAGS,
    non_matched_records.MILEAGE,
    non_matched_records.MILEAGE_TYPE,
    non_matched_records.EXT_COLOR,
    non_matched_records.FIRST_NAME,
    non_matched_records.LAST_NAME,
    non_matched_records.ADDRESS,
    non_matched_records.CITY,
    non_matched_records.STATE,
    non_matched_records.POSTAL,
    non_matched_records.PHONE,
    non_matched_records.MOBILE,
    non_matched_records.EMAIL,
    non_matched_records.OTHER_EMAIL,
    non_matched_records.SALESPERSON,
    non_matched_records.GVWR,
    non_matched_records.DESCRIPTION,
    non_matched_records._FIVETRAN_SYNCED
    FROM non_matched_records)



SELECT
    _FILE,
    _LINE,
    _MODIFIED,
    TYPE,
    STOCK_NUMBER,
    MAKE,
    MODEL,
    SUBMODEL,
    YEAR,
    STATUS,
    ORDER_NUMBER,
    TOTAL_PRICE,
    DEAL_TYPE,
    CUSTOMER,
    DATE_SOLD,
    BASE_PRICE,
    FACTORY_OPTIONS,
    DEALER_INSTALLED,
    PART_OPTIONS,
    LABOR_OPTIONS,
    SUBLET_OPTIONS,
    FEE_OPTIONS,
    WARRANTY_OPTIONS,
    OTHER_PRODUCTS,
    STAMP_DUTY,
    VIN,
    NEW_UNIT,
    CATEGORY,
    STOCKED_IN,
    AGE,
    TAGS,
    MILEAGE,
    MILEAGE_TYPE,
    EXT_COLOR,
    FIRST_NAME,
    LAST_NAME,
    ADDRESS,
    CITY,
    STATE,
    POSTAL,
    PHONE,
    MOBILE,
    EMAIL,
    OTHER_EMAIL,
    SALESPERSON,
    GVWR,
    DESCRIPTION,
    _FIVETRAN_SYNCED
FROM updates

UNION

SELECT
    _FILE,
    _LINE,
    _MODIFIED,
    TYPE,
    STOCK_NUMBER,
    MAKE,
    MODEL,
    SUBMODEL,
    YEAR,
    STATUS,
    ORDER_NUMBER,
    TOTAL_PRICE,
    DEAL_TYPE,
    CUSTOMER,
    DATE_SOLD,
    BASE_PRICE,
    FACTORY_OPTIONS,
    DEALER_INSTALLED,
    PART_OPTIONS,
    LABOR_OPTIONS,
    SUBLET_OPTIONS,
    FEE_OPTIONS,
    WARRANTY_OPTIONS,
    OTHER_PRODUCTS,
    STAMP_DUTY,
    VIN,
    NEW_UNIT,
    CATEGORY,
    STOCKED_IN,
    AGE,
    TAGS,
    MILEAGE,
    MILEAGE_TYPE,
    EXT_COLOR,
    FIRST_NAME,
    LAST_NAME,
    ADDRESS,
    CITY,
    STATE,
    POSTAL,
    PHONE,
    MOBILE,
    EMAIL,
    OTHER_EMAIL,
    SALESPERSON,
    GVWR,
    DESCRIPTION,
    _FIVETRAN_SYNCED
FROM inserts