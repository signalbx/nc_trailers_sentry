{{ config(
    schema='BLACKPURL_PRODUCTION',
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['INVOICE_NUMBER', 'INVOICE_DATE', 'ITEM_DESCRIPTION'],
    pre_hook=[
        "{{ delete_duplicates_invoice_detail('blackpurl_production', 'invoice_detail_stage', 'temp_invoice_detail_stage_table') }}",
        "{{ delete_duplicates_invoice_detail('blackpurl_production_blackpurl_production', 'invoice_detail_prod', 'temp_invoice_detail_prod_table') }}",
    ],

    post_hook="TRUNCATE TABLE {{ source('blackpurl_production', 'invoice_detail_stage') }};"

) }}


WITH stage AS (
    SELECT 
    _FILE,
    _LINE,
    _MODIFIED,
    TYPE,
    INVOICE_NUMBER,
    COALESCE(TRY_TO_DATE(INVOICE_DATE, 'MM/DD/YYYY'), 
                TRY_TO_DATE(INVOICE_DATE, 'M/D/YYYY'), 
                TRY_TO_DATE(INVOICE_DATE, 'MM-DD-YYYY'), 
                TRY_TO_DATE(INVOICE_DATE, 'M-D-YYYY'), 
                TRY_TO_DATE(INVOICE_DATE, 'YYYY-MM-DD'),
                TRY_TO_DATE(INVOICE_DATE, 'MM/DD/YYYY HH24:MI'),
                TRY_TO_DATE(INVOICE_DATE, 'MM/DD/YYYY HH12:MI AM')) 
        AS formatted_date,
    CUSTOMER_NAME,
    SALESPERSON,
    SALE_TYPE,
    ITEM_TYPE,
    ITEM_DESCRIPTION,
    QTY,
    TRY_CAST(REPLACE(REPLACE(TOTAL_PRICE, '$', ''), ',', '') AS FLOAT) AS TOTAL_PRICE,
    TRY_CAST(REPLACE(REPLACE(TOTAL_COST, '$', ''), ',', '') AS FLOAT) AS TOTAL_COST,
    TRY_CAST(REPLACE(REPLACE(TOTAL_PROFIT, '$', ''), ',', '') AS FLOAT) AS TOTAL_PROFIT,
    TOTAL_GROSS_PCT,
    TRY_CAST(REPLACE(REPLACE(PART_SALES, '$', ''), ',', '') AS FLOAT) AS PART_SALES,
    TRY_CAST(REPLACE(REPLACE(LABOR_SALES, '$', ''), ',', '') AS FLOAT) AS LABOR_SALES,
    TRY_CAST(REPLACE(REPLACE(SUBLET_SALES, '$', ''), ',', '') AS FLOAT) AS SUBLET_SALES,
    TRY_CAST(REPLACE(REPLACE(FEE_SALES, '$', ''), ',', '') AS FLOAT) AS FEE_SALES,
    TRY_CAST(REPLACE(REPLACE(SUPPLIES_SALES, '$', ''), ',', '') AS FLOAT) AS SUPPLIES_SALES,
    TRY_CAST(REPLACE(REPLACE(UNIT_BASE_SALES, '$', ''), ',', '') AS FLOAT) AS UNIT_BASE_SALES,
    TRY_CAST(REPLACE(REPLACE(UNIT_FACTORY_SALES, '$', ''), ',', '') AS FLOAT) AS UNIT_FACTORY_SALES,
    TRY_CAST(REPLACE(REPLACE(UNIT_DEALER_SALES, '$', ''), ',', '') AS FLOAT) AS UNIT_DEALER_SALES,
    TRY_CAST(REPLACE(REPLACE(OPTION_PART_SALES, '$', ''), ',', '') AS FLOAT) AS OPTION_PART_SALES,
    TRY_CAST(REPLACE(REPLACE(OPTION_LABOR_SALES, '$', ''), ',', '') AS FLOAT) AS OPTION_LABOR_SALES,
    TRY_CAST(REPLACE(REPLACE(OPTION_SUBLET_SALES, '$', ''), ',', '') AS FLOAT) AS OPTION_SUBLET_SALES,
    TRY_CAST(REPLACE(REPLACE(OPTION_FEE_SALES, '$', ''), ',', '') AS FLOAT) AS OPTION_FEE_SALES,
    TRY_CAST(REPLACE(REPLACE(OPTION_WARRANTY_SALES, '$', ''), ',', '') AS FLOAT) AS OPTION_WARRANTY_SALES,
    TRY_CAST(REPLACE(REPLACE(OPTION_PRODUCT_SALES, '$', ''), ',', '') AS FLOAT) AS OPTION_PRODUCT_SALES,
    TRY_CAST(REPLACE(REPLACE(TRADE_INS, '$', ''), ',', '') AS FLOAT) AS TRADE_INS,
    TRY_CAST(REPLACE(REPLACE(FINANCE_PRODUCT_SALES, '$', ''), ',', '') AS FLOAT) AS FINANCE_PRODUCT_SALES,
    _FIVETRAN_SYNCED
    FROM {{ source('blackpurl_production', 'invoice_detail_stage') }}
    where formatted_date is not null
),

prod AS ( 
    SELECT 
    _FILE,
    _LINE,
    _MODIFIED,
    TYPE,
    INVOICE_NUMBER,
    COALESCE(TRY_TO_DATE(INVOICE_DATE, 'MM/DD/YYYY'), 
                TRY_TO_DATE(INVOICE_DATE, 'M/D/YYYY'), 
                TRY_TO_DATE(INVOICE_DATE, 'MM-DD-YYYY'), 
                TRY_TO_DATE(INVOICE_DATE, 'M-D-YYYY'), 
                TRY_TO_DATE(INVOICE_DATE, 'YYYY-MM-DD'),
                TRY_TO_DATE(INVOICE_DATE, 'MM/DD/YYYY HH24:MI'),
                TRY_TO_DATE(INVOICE_DATE, 'MM/DD/YYYY HH12:MI AM')) 
        AS formatted_date,
    CUSTOMER_NAME,
    SALESPERSON,
    SALE_TYPE,
    ITEM_TYPE,
    ITEM_DESCRIPTION,
    QTY,
    TRY_CAST(REPLACE(REPLACE(TOTAL_PRICE, '$', ''), ',', '') AS FLOAT) AS TOTAL_PRICE,
    TRY_CAST(REPLACE(REPLACE(TOTAL_COST, '$', ''), ',', '') AS FLOAT) AS TOTAL_COST,
    TRY_CAST(REPLACE(REPLACE(TOTAL_PROFIT, '$', ''), ',', '') AS FLOAT) AS TOTAL_PROFIT,
    TOTAL_GROSS_PCT,
    TRY_CAST(REPLACE(REPLACE(PART_SALES, '$', ''), ',', '') AS FLOAT) AS PART_SALES,
    TRY_CAST(REPLACE(REPLACE(LABOR_SALES, '$', ''), ',', '') AS FLOAT) AS LABOR_SALES,
    TRY_CAST(REPLACE(REPLACE(SUBLET_SALES, '$', ''), ',', '') AS FLOAT) AS SUBLET_SALES,
    TRY_CAST(REPLACE(REPLACE(FEE_SALES, '$', ''), ',', '') AS FLOAT) AS FEE_SALES,
    TRY_CAST(REPLACE(REPLACE(SUPPLIES_SALES, '$', ''), ',', '') AS FLOAT) AS SUPPLIES_SALES,
    TRY_CAST(REPLACE(REPLACE(UNIT_BASE_SALES, '$', ''), ',', '') AS FLOAT) AS UNIT_BASE_SALES,
    TRY_CAST(REPLACE(REPLACE(UNIT_FACTORY_SALES, '$', ''), ',', '') AS FLOAT) AS UNIT_FACTORY_SALES,
    TRY_CAST(REPLACE(REPLACE(UNIT_DEALER_SALES, '$', ''), ',', '') AS FLOAT) AS UNIT_DEALER_SALES,
    TRY_CAST(REPLACE(REPLACE(OPTION_PART_SALES, '$', ''), ',', '') AS FLOAT) AS OPTION_PART_SALES,
    TRY_CAST(REPLACE(REPLACE(OPTION_LABOR_SALES, '$', ''), ',', '') AS FLOAT) AS OPTION_LABOR_SALES,
    TRY_CAST(REPLACE(REPLACE(OPTION_SUBLET_SALES, '$', ''), ',', '') AS FLOAT) AS OPTION_SUBLET_SALES,
    TRY_CAST(REPLACE(REPLACE(OPTION_FEE_SALES, '$', ''), ',', '') AS FLOAT) AS OPTION_FEE_SALES,
    TRY_CAST(REPLACE(REPLACE(OPTION_WARRANTY_SALES, '$', ''), ',', '') AS FLOAT) AS OPTION_WARRANTY_SALES,
    TRY_CAST(REPLACE(REPLACE(OPTION_PRODUCT_SALES, '$', ''), ',', '') AS FLOAT) AS OPTION_PRODUCT_SALES,
    TRY_CAST(REPLACE(REPLACE(TRADE_INS, '$', ''), ',', '') AS FLOAT) AS TRADE_INS,
    TRY_CAST(REPLACE(REPLACE(FINANCE_PRODUCT_SALES, '$', ''), ',', '') AS FLOAT) AS FINANCE_PRODUCT_SALES,
    _FIVETRAN_SYNCED
    FROM {{ source('blackpurl_production_blackpurl_production', 'invoice_detail_prod') }}
    where formatted_date is not null
),

matched_records AS (
    SELECT
        stage.*
    FROM stage
    JOIN prod
        ON stage.INVOICE_NUMBER = prod.INVOICE_NUMBER
        AND stage.formatted_date = prod.formatted_date
        AND stage.ITEM_DESCRIPTION = prod.ITEM_DESCRIPTION
),

updates AS (
    SELECT
    matched_records._FILE,
    matched_records._LINE,
    matched_records._MODIFIED,
    matched_records.TYPE,
    matched_records.INVOICE_NUMBER,
    matched_records.formatted_date as INVOICE_DATE,
    matched_records.CUSTOMER_NAME,
    matched_records.SALESPERSON,
    matched_records.SALE_TYPE,
    matched_records.ITEM_TYPE,
    matched_records.ITEM_DESCRIPTION,
    matched_records.QTY,
    matched_records.TOTAL_PRICE,
    matched_records.TOTAL_COST,
    matched_records.TOTAL_PROFIT,
    matched_records.TOTAL_GROSS_PCT,
    matched_records.PART_SALES,
    matched_records.LABOR_SALES,
    matched_records.SUBLET_SALES,
    matched_records.FEE_SALES,
    matched_records.SUPPLIES_SALES,
    matched_records.UNIT_BASE_SALES,
    matched_records.UNIT_FACTORY_SALES,
    matched_records.UNIT_DEALER_SALES,
    matched_records.OPTION_PART_SALES,
    matched_records.OPTION_LABOR_SALES,
    matched_records.OPTION_SUBLET_SALES,
    matched_records.OPTION_FEE_SALES,
    matched_records.OPTION_WARRANTY_SALES,
    matched_records.OPTION_PRODUCT_SALES,
    matched_records.TRADE_INS,
    matched_records.FINANCE_PRODUCT_SALES,
    matched_records._FIVETRAN_SYNCED
    FROM matched_records),

non_matched_records AS (
    SELECT
        stage.*
    FROM stage
    LEFT JOIN prod
        ON stage.INVOICE_NUMBER = prod.INVOICE_NUMBER
        AND stage.formatted_date = prod.formatted_date
        AND stage.ITEM_DESCRIPTION = prod.ITEM_DESCRIPTION
    WHERE prod.INVOICE_NUMBER IS NULL AND prod.formatted_date IS NULL AND prod.ITEM_DESCRIPTION IS NULL
),

inserts AS (
    SELECT
    non_matched_records._FILE,
    non_matched_records._LINE,
    non_matched_records._MODIFIED,
    non_matched_records.TYPE,
    non_matched_records.INVOICE_NUMBER,
    non_matched_records.formatted_date as INVOICE_DATE,
    non_matched_records.CUSTOMER_NAME,
    non_matched_records.SALESPERSON,
    non_matched_records.SALE_TYPE,
    non_matched_records.ITEM_TYPE,
    non_matched_records.ITEM_DESCRIPTION,
    non_matched_records.QTY,
    non_matched_records.TOTAL_PRICE,
    non_matched_records.TOTAL_COST,
    non_matched_records.TOTAL_PROFIT,
    non_matched_records.TOTAL_GROSS_PCT,
    non_matched_records.PART_SALES,
    non_matched_records.LABOR_SALES,
    non_matched_records.SUBLET_SALES,
    non_matched_records.FEE_SALES,
    non_matched_records.SUPPLIES_SALES,
    non_matched_records.UNIT_BASE_SALES,
    non_matched_records.UNIT_FACTORY_SALES,
    non_matched_records.UNIT_DEALER_SALES,
    non_matched_records.OPTION_PART_SALES,
    non_matched_records.OPTION_LABOR_SALES,
    non_matched_records.OPTION_SUBLET_SALES,
    non_matched_records.OPTION_FEE_SALES,
    non_matched_records.OPTION_WARRANTY_SALES,
    non_matched_records.OPTION_PRODUCT_SALES,
    non_matched_records.TRADE_INS,
    non_matched_records.FINANCE_PRODUCT_SALES,
    non_matched_records._FIVETRAN_SYNCED
    FROM non_matched_records)



SELECT
    _FILE,
    _LINE,
    _MODIFIED,
    TYPE,
    INVOICE_NUMBER,
    INVOICE_DATE,
    CUSTOMER_NAME,
    SALESPERSON,
    SALE_TYPE,
    ITEM_TYPE,
    ITEM_DESCRIPTION,
    QTY,
    TOTAL_PRICE,
    TOTAL_COST,
    TOTAL_PROFIT,
    TOTAL_GROSS_PCT,
    PART_SALES,
    LABOR_SALES,
    SUBLET_SALES,
    FEE_SALES,
    SUPPLIES_SALES,
    UNIT_BASE_SALES,
    UNIT_FACTORY_SALES,
    UNIT_DEALER_SALES,
    OPTION_PART_SALES,
    OPTION_LABOR_SALES,
    OPTION_SUBLET_SALES,
    OPTION_FEE_SALES,
    OPTION_WARRANTY_SALES,
    OPTION_PRODUCT_SALES,
    TRADE_INS,
    FINANCE_PRODUCT_SALES,
    _FIVETRAN_SYNCED
FROM updates

UNION

SELECT
    _FILE,
    _LINE,
    _MODIFIED,
    TYPE,
    INVOICE_NUMBER,
    INVOICE_DATE,
    CUSTOMER_NAME,
    SALESPERSON,
    SALE_TYPE,
    ITEM_TYPE,
    ITEM_DESCRIPTION,
    QTY,
    TOTAL_PRICE,
    TOTAL_COST,
    TOTAL_PROFIT,
    TOTAL_GROSS_PCT,
    PART_SALES,
    LABOR_SALES,
    SUBLET_SALES,
    FEE_SALES,
    SUPPLIES_SALES,
    UNIT_BASE_SALES,
    UNIT_FACTORY_SALES,
    UNIT_DEALER_SALES,
    OPTION_PART_SALES,
    OPTION_LABOR_SALES,
    OPTION_SUBLET_SALES,
    OPTION_FEE_SALES,
    OPTION_WARRANTY_SALES,
    OPTION_PRODUCT_SALES,
    TRADE_INS,
    FINANCE_PRODUCT_SALES,
    _FIVETRAN_SYNCED
FROM inserts