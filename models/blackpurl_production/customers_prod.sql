{{ config(
    schema='BLACKPURL_PRODUCTION',
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['CUSTOMER_NUMBER'],
    pre_hook=[
        "{{ delete_duplicates_customers('blackpurl_production', 'customers_stage', 'temp_customers_stage_table') }}",
        "{{ delete_duplicates_customers('blackpurl_production_blackpurl_production', 'customers_prod', 'temp_customers_prod_table') }}",
    ],

    post_hook="TRUNCATE TABLE {{ source('blackpurl_production', 'customers_stage') }};"

) }}


WITH stage AS (
    SELECT 
    _FILE,
    _LINE,
    _MODIFIED,
    TYPE,
    CUSTOMER_NUMBER,
    CUSTOMER_NAME,
    PHONE_NUMBER,
    EMAIL,
    OWNER,
    MODIFIED,
    FIRST_NAME,
    LAST_NAME,
    NICKNAME,
    BIRTH_DATE,
    JOB_TITLE,
    BILLING_ADDRESS,
    BILLING_CITY,
    BILLING_STATE,
    SHIPPING_ADDRESS,
    SHIPPING_CITY,
    SHIPPING_STATE,
    MOBILE_NUMBER,
    OTHER_EMAIL,
    WORK_EMAIL,
    PRICE_LEVEL,
    TRY_CAST(REPLACE(REPLACE(OPEN_ORDERS, '$', ''), ',', '') AS FLOAT) AS OPEN_ORDERS,
    TRY_CAST(REPLACE(REPLACE(ORDERS_LIFETIME, '$', ''), ',', '') AS FLOAT) AS ORDERS_LIFETIME,
    TRY_CAST(REPLACE(REPLACE(AVERAGE_SPEND_PER_ORDER, '$', ''), ',', '') AS FLOAT) AS AVERAGE_SPEND_PER_ORDER,
    CREATED,
    POSTAL_CODE,
    ACCOUNT_TYPE,
    DRIVERS_LICENSE,
    _FIVETRAN_SYNCED,
    TRY_CAST(REPLACE(REPLACE(STORE_CREDITS, '$', ''), ',', '') AS FLOAT) AS STORE_CREDITS
    FROM {{ source('blackpurl_production', 'customers_stage') }}
),

prod AS (
    SELECT 
    _FILE,
    _LINE,
    _MODIFIED,
    TYPE,
    CUSTOMER_NUMBER,
    CUSTOMER_NAME,
    PHONE_NUMBER,
    EMAIL,
    OWNER,
    MODIFIED,
    FIRST_NAME,
    LAST_NAME,
    NICKNAME,
    BIRTH_DATE,
    JOB_TITLE,
    BILLING_ADDRESS,
    BILLING_CITY,
    BILLING_STATE,
    SHIPPING_ADDRESS,
    SHIPPING_CITY,
    SHIPPING_STATE,
    MOBILE_NUMBER,
    OTHER_EMAIL,
    WORK_EMAIL,
    PRICE_LEVEL,
    TRY_CAST(REPLACE(REPLACE(OPEN_ORDERS, '$', ''), ',', '') AS FLOAT) AS OPEN_ORDERS,
    TRY_CAST(REPLACE(REPLACE(ORDERS_LIFETIME, '$', ''), ',', '') AS FLOAT) AS ORDERS_LIFETIME,
    TRY_CAST(REPLACE(REPLACE(AVERAGE_SPEND_PER_ORDER, '$', ''), ',', '') AS FLOAT) AS AVERAGE_SPEND_PER_ORDER,
    CREATED,
    POSTAL_CODE,
    ACCOUNT_TYPE,
    DRIVERS_LICENSE,
    _FIVETRAN_SYNCED,
    TRY_CAST(REPLACE(REPLACE(STORE_CREDITS, '$', ''), ',', '') AS FLOAT) AS STORE_CREDITS
    FROM {{ source('blackpurl_production_blackpurl_production', 'customers_prod') }}
),

matched_records AS (
    SELECT
        stage.*
    FROM stage
    JOIN prod
        ON stage.CUSTOMER_NUMBER = prod.CUSTOMER_NUMBER
),

updates AS (
    SELECT
        matched_records._FILE,
    matched_records._LINE,
    matched_records._MODIFIED,
    matched_records.TYPE,
    matched_records.CUSTOMER_NUMBER,
    matched_records.CUSTOMER_NAME,
    matched_records.PHONE_NUMBER,
    matched_records.EMAIL,
    matched_records.OWNER,
    matched_records.MODIFIED,
    matched_records.FIRST_NAME,
    matched_records.LAST_NAME,
    matched_records.NICKNAME,
    matched_records.BIRTH_DATE,
    matched_records.JOB_TITLE,
    matched_records.BILLING_ADDRESS,
    matched_records.BILLING_CITY,
    matched_records.BILLING_STATE,
    matched_records.SHIPPING_ADDRESS,
    matched_records.SHIPPING_CITY,
    matched_records.SHIPPING_STATE,
    matched_records.MOBILE_NUMBER,
    matched_records.OTHER_EMAIL,
    matched_records.WORK_EMAIL,
    matched_records.PRICE_LEVEL,
    matched_records.OPEN_ORDERS,
    matched_records.ORDERS_LIFETIME,
    matched_records.AVERAGE_SPEND_PER_ORDER,
    matched_records.CREATED,
    matched_records.POSTAL_CODE,
    matched_records.ACCOUNT_TYPE,
    matched_records.DRIVERS_LICENSE,
    matched_records._FIVETRAN_SYNCED,
    matched_records.STORE_CREDITS
    FROM matched_records),

non_matched_records AS (
    SELECT
        stage.*
    FROM stage
    LEFT JOIN prod
        ON stage.CUSTOMER_NUMBER = prod.CUSTOMER_NUMBER
    WHERE prod.CUSTOMER_NUMBER IS NULL
),

inserts AS (
    SELECT
    non_matched_records._FILE,
    non_matched_records._LINE,
    non_matched_records._MODIFIED,
    non_matched_records.TYPE,
    non_matched_records.CUSTOMER_NUMBER,
    non_matched_records.CUSTOMER_NAME,
    non_matched_records.PHONE_NUMBER,
    non_matched_records.EMAIL,
    non_matched_records.OWNER,
    non_matched_records.MODIFIED,
    non_matched_records.FIRST_NAME,
    non_matched_records.LAST_NAME,
    non_matched_records.NICKNAME,
    non_matched_records.BIRTH_DATE,
    non_matched_records.JOB_TITLE,
    non_matched_records.BILLING_ADDRESS,
    non_matched_records.BILLING_CITY,
    non_matched_records.BILLING_STATE,
    non_matched_records.SHIPPING_ADDRESS,
    non_matched_records.SHIPPING_CITY,
    non_matched_records.SHIPPING_STATE,
    non_matched_records.MOBILE_NUMBER,
    non_matched_records.OTHER_EMAIL,
    non_matched_records.WORK_EMAIL,
    non_matched_records.PRICE_LEVEL,
    non_matched_records.OPEN_ORDERS,
    non_matched_records.ORDERS_LIFETIME,
    non_matched_records.AVERAGE_SPEND_PER_ORDER,
    non_matched_records.CREATED,
    non_matched_records.POSTAL_CODE,
    non_matched_records.ACCOUNT_TYPE,
    non_matched_records.DRIVERS_LICENSE,
    non_matched_records._FIVETRAN_SYNCED,
    non_matched_records.STORE_CREDITS
    FROM non_matched_records)



SELECT
    _FILE,
    _LINE,
    _MODIFIED,
    TYPE,
    CUSTOMER_NUMBER,
    CUSTOMER_NAME,
    PHONE_NUMBER,
    EMAIL,
    OWNER,
    MODIFIED,
    FIRST_NAME,
    LAST_NAME,
    NICKNAME,
    BIRTH_DATE,
    JOB_TITLE,
    BILLING_ADDRESS,
    BILLING_CITY,
    BILLING_STATE,
    SHIPPING_ADDRESS,
    SHIPPING_CITY,
    SHIPPING_STATE,
    MOBILE_NUMBER,
    OTHER_EMAIL,
    WORK_EMAIL,
    PRICE_LEVEL,
    OPEN_ORDERS,
    ORDERS_LIFETIME,
    AVERAGE_SPEND_PER_ORDER,
    CREATED,
    POSTAL_CODE,
    ACCOUNT_TYPE,
    DRIVERS_LICENSE,
    _FIVETRAN_SYNCED,
    STORE_CREDITS
FROM updates

UNION

SELECT
    _FILE,
    _LINE,
    _MODIFIED,
    TYPE,
    CUSTOMER_NUMBER,
    CUSTOMER_NAME,
    PHONE_NUMBER,
    EMAIL,
    OWNER,
    MODIFIED,
    FIRST_NAME,
    LAST_NAME,
    NICKNAME,
    BIRTH_DATE,
    JOB_TITLE,
    BILLING_ADDRESS,
    BILLING_CITY,
    BILLING_STATE,
    SHIPPING_ADDRESS,
    SHIPPING_CITY,
    SHIPPING_STATE,
    MOBILE_NUMBER,
    OTHER_EMAIL,
    WORK_EMAIL,
    PRICE_LEVEL,
    OPEN_ORDERS,
    ORDERS_LIFETIME,
    AVERAGE_SPEND_PER_ORDER,
    CREATED,
    POSTAL_CODE,
    ACCOUNT_TYPE,
    DRIVERS_LICENSE,
    _FIVETRAN_SYNCED,
    STORE_CREDITS
FROM inserts
