{{ config(
    schema='BLACKPURL_PRODUCTION',
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['INVOICE_NUMBER', 'INVOICE_DATE'],
    pre_hook=[
        "{{ delete_duplicates_invoice_summary('blackpurl_production', 'invoice_summary_stage', 'temp_invoice_summary_stage_table') }}",
        "{{ delete_duplicates_invoice_summary('blackpurl_production_blackpurl_production', 'invoice_summary_prod', 'temp_invoice_summary_prod_table') }}",
    ],

    post_hook="TRUNCATE TABLE {{ source('blackpurl_production', 'invoice_summary_stage') }};"

) }}


WITH stage AS (
    SELECT 
    _FILE,
    _LINE,
    _MODIFIED,
    TYPE,
    COALESCE(TRY_TO_DATE(INVOICE_DATE, 'MM/DD/YYYY'), 
                TRY_TO_DATE(INVOICE_DATE, 'M/D/YYYY'), 
                TRY_TO_DATE(INVOICE_DATE, 'MM-DD-YYYY'), 
                TRY_TO_DATE(INVOICE_DATE, 'M-D-YYYY'), 
                TRY_TO_DATE(INVOICE_DATE, 'YYYY-MM-DD'),
                TRY_TO_DATE(INVOICE_DATE, 'MM/DD/YYYY HH24:MI'),
                TRY_TO_DATE(INVOICE_DATE, 'MM/DD/YYYY HH12:MI AM')) 
        AS formatted_date,
    INVOICE_NUMBER,
    ORDER_NUMBER,
    CUSTOMER_NAME,
    TRY_CAST(REPLACE(REPLACE(PART_TOTAL, '$', ''), ',', '') AS FLOAT) as PART_TOTAL,
    TRY_CAST(REPLACE(REPLACE(LABOR_TOTAL, '$', ''), ',', '') AS FLOAT) as LABOR_TOTAL,
    TRY_CAST(REPLACE(REPLACE(SUBLET_TOTAL, '$', ''), ',', '') AS FLOAT) as SUBLET_TOTAL,
    TRY_CAST(REPLACE(REPLACE(FEE_TOTAL, '$', ''), ',', '') AS FLOAT) as FEE_TOTAL,
    TRY_CAST(REPLACE(REPLACE(SALES_TAX_TOTAL, '$', ''), ',', '') AS FLOAT) as SALES_TAX_TOTAL,
    TRY_CAST(REPLACE(REPLACE(TOTAL, '$', ''), ',', '') AS FLOAT) as TOTAL,
    OWNER,
    STATUS,
    CREATED,
    MODIFIED,
    TRY_CAST(REPLACE(REPLACE(UNIT_TOTAL, '$', ''), ',', '') AS FLOAT) as UNIT_TOTAL,
    TRY_CAST(REPLACE(REPLACE(TRADE_IN_TOTAL, '$', ''), ',', '') AS FLOAT) as TRADE_IN_TOTAL,
    TRY_CAST(REPLACE(REPLACE(WARRANTY_PRODUCT_TOTAL, '$', ''), ',', '') AS FLOAT) as WARRANTY_PRODUCT_TOTAL,
    TRY_CAST(REPLACE(REPLACE(OTHER_PRODUCT_TOTAL, '$', ''), ',', '') AS FLOAT) as OTHER_PRODUCT_TOTAL,
    TRY_CAST(REPLACE(REPLACE(INVOICE_PROFIT, '$', ''), ',', '') AS FLOAT) as INVOICE_PROFIT,
    TRY_CAST(REPLACE(REPLACE(DEAL_PROFIT, '$', ''), ',', '') AS FLOAT) as DEAL_PROFIT,
    TRY_CAST(REPLACE(REPLACE(TAXABLE_TOTAL, '$', ''), ',', '') AS FLOAT) as TAXABLE_TOTAL,
    TRY_CAST(REPLACE(REPLACE(NON_TAXABLE_TOTAL, '$', ''), ',', '') AS FLOAT) as NON_TAXABLE_TOTAL,
    TRY_CAST(REPLACE(REPLACE(SALES_COMMISSION, '$', ''), ',', '') AS FLOAT) as SALES_COMMISSION,
    SALESPERSON,
    ACCOUNT_TYPE,
    TRY_CAST(REPLACE(REPLACE(LIEN_PAYOUT, '$', ''), ',', '') AS FLOAT) as LIEN_PAYOUT,
    TRY_CAST(REPLACE(REPLACE(FINANCE_COMMISSION, '$', ''), ',', '') AS FLOAT) as FINANCE_COMMISSION,
    _FIVETRAN_SYNCED
    FROM {{ source('blackpurl_production', 'invoice_summary_stage') }}
    where formatted_date is not null
),

prod AS ( 
    SELECT 
    _FILE,
    _LINE,
    _MODIFIED,
    TYPE,
    COALESCE(TRY_TO_DATE(INVOICE_DATE, 'MM/DD/YYYY'), 
                TRY_TO_DATE(INVOICE_DATE, 'M/D/YYYY'), 
                TRY_TO_DATE(INVOICE_DATE, 'MM-DD-YYYY'), 
                TRY_TO_DATE(INVOICE_DATE, 'M-D-YYYY'), 
                TRY_TO_DATE(INVOICE_DATE, 'YYYY-MM-DD'),
                TRY_TO_DATE(INVOICE_DATE, 'MM/DD/YYYY HH24:MI'),
                TRY_TO_DATE(INVOICE_DATE, 'MM/DD/YYYY HH12:MI AM')) 
        AS formatted_date,
    INVOICE_NUMBER,
    ORDER_NUMBER,
    CUSTOMER_NAME,
    TRY_CAST(REPLACE(REPLACE(PART_TOTAL, '$', ''), ',', '') AS FLOAT) as PART_TOTAL,
    TRY_CAST(REPLACE(REPLACE(LABOR_TOTAL, '$', ''), ',', '') AS FLOAT) as LABOR_TOTAL,
    TRY_CAST(REPLACE(REPLACE(SUBLET_TOTAL, '$', ''), ',', '') AS FLOAT) as SUBLET_TOTAL,
    TRY_CAST(REPLACE(REPLACE(FEE_TOTAL, '$', ''), ',', '') AS FLOAT) as FEE_TOTAL,
    TRY_CAST(REPLACE(REPLACE(SALES_TAX_TOTAL, '$', ''), ',', '') AS FLOAT) as SALES_TAX_TOTAL,
    TRY_CAST(REPLACE(REPLACE(TOTAL, '$', ''), ',', '') AS FLOAT) as TOTAL,
    OWNER,
    STATUS,
    CREATED,
    MODIFIED,
    TRY_CAST(REPLACE(REPLACE(UNIT_TOTAL, '$', ''), ',', '') AS FLOAT) as UNIT_TOTAL,
    TRY_CAST(REPLACE(REPLACE(TRADE_IN_TOTAL, '$', ''), ',', '') AS FLOAT) as TRADE_IN_TOTAL,
    TRY_CAST(REPLACE(REPLACE(WARRANTY_PRODUCT_TOTAL, '$', ''), ',', '') AS FLOAT) as WARRANTY_PRODUCT_TOTAL,
    TRY_CAST(REPLACE(REPLACE(OTHER_PRODUCT_TOTAL, '$', ''), ',', '') AS FLOAT) as OTHER_PRODUCT_TOTAL,
    TRY_CAST(REPLACE(REPLACE(INVOICE_PROFIT, '$', ''), ',', '') AS FLOAT) as INVOICE_PROFIT,
    TRY_CAST(REPLACE(REPLACE(DEAL_PROFIT, '$', ''), ',', '') AS FLOAT) as DEAL_PROFIT,
    TRY_CAST(REPLACE(REPLACE(TAXABLE_TOTAL, '$', ''), ',', '') AS FLOAT) as TAXABLE_TOTAL,
    TRY_CAST(REPLACE(REPLACE(NON_TAXABLE_TOTAL, '$', ''), ',', '') AS FLOAT) as NON_TAXABLE_TOTAL,
    TRY_CAST(REPLACE(REPLACE(SALES_COMMISSION, '$', ''), ',', '') AS FLOAT) as SALES_COMMISSION,
    SALESPERSON,
    ACCOUNT_TYPE,
    TRY_CAST(REPLACE(REPLACE(LIEN_PAYOUT, '$', ''), ',', '') AS FLOAT) as LIEN_PAYOUT,
    TRY_CAST(REPLACE(REPLACE(FINANCE_COMMISSION, '$', ''), ',', '') AS FLOAT) as FINANCE_COMMISSION,
    _FIVETRAN_SYNCED
    FROM {{ source('blackpurl_production_blackpurl_production', 'invoice_summary_prod') }}
    where formatted_date is not null
),

matched_records AS (
    SELECT
        stage.*
    FROM stage
    JOIN prod
        ON stage.INVOICE_NUMBER = prod.INVOICE_NUMBER
        AND stage.formatted_date = prod.formatted_date
),

updates AS (
    SELECT
    matched_records._FILE,
    matched_records._LINE,
    matched_records._MODIFIED,
    matched_records.TYPE,
    matched_records.formatted_date as INVOICE_DATE,
    matched_records.INVOICE_NUMBER,
    matched_records.ORDER_NUMBER,
    matched_records.CUSTOMER_NAME,
    matched_records.PART_TOTAL,
    matched_records.LABOR_TOTAL,
    matched_records.SUBLET_TOTAL,
    matched_records.FEE_TOTAL,
    matched_records.SALES_TAX_TOTAL,
    matched_records.TOTAL,
    matched_records.OWNER,
    matched_records.STATUS,
    matched_records.CREATED,
    matched_records.MODIFIED,
    matched_records.UNIT_TOTAL,
    matched_records.TRADE_IN_TOTAL,
    matched_records.WARRANTY_PRODUCT_TOTAL,
    matched_records.OTHER_PRODUCT_TOTAL,
    matched_records.INVOICE_PROFIT,
    matched_records.DEAL_PROFIT,
    matched_records.TAXABLE_TOTAL,
    matched_records.NON_TAXABLE_TOTAL,
    matched_records.SALES_COMMISSION,
    matched_records.SALESPERSON,
    matched_records.ACCOUNT_TYPE,
    matched_records.LIEN_PAYOUT,
    matched_records.FINANCE_COMMISSION,
    matched_records._FIVETRAN_SYNCED
    FROM matched_records),

non_matched_records AS (
    SELECT
        stage.*
    FROM stage
    LEFT JOIN prod
        ON stage.INVOICE_NUMBER = prod.INVOICE_NUMBER
        AND stage.formatted_date = prod.formatted_date
    WHERE prod.INVOICE_NUMBER IS NULL AND prod.formatted_date IS NULL
),

inserts AS (
    SELECT
    non_matched_records._FILE,
    non_matched_records._LINE,
    non_matched_records._MODIFIED,
    non_matched_records.TYPE,
    non_matched_records.formatted_date as INVOICE_DATE,
    non_matched_records.INVOICE_NUMBER,
    non_matched_records.ORDER_NUMBER,
    non_matched_records.CUSTOMER_NAME,
    non_matched_records.PART_TOTAL,
    non_matched_records.LABOR_TOTAL,
    non_matched_records.SUBLET_TOTAL,
    non_matched_records.FEE_TOTAL,
    non_matched_records.SALES_TAX_TOTAL,
    non_matched_records.TOTAL,
    non_matched_records.OWNER,
    non_matched_records.STATUS,
    non_matched_records.CREATED,
    non_matched_records.MODIFIED,
    non_matched_records.UNIT_TOTAL,
    non_matched_records.TRADE_IN_TOTAL,
    non_matched_records.WARRANTY_PRODUCT_TOTAL,
    non_matched_records.OTHER_PRODUCT_TOTAL,
    non_matched_records.INVOICE_PROFIT,
    non_matched_records.DEAL_PROFIT,
    non_matched_records.TAXABLE_TOTAL,
    non_matched_records.NON_TAXABLE_TOTAL,
    non_matched_records.SALES_COMMISSION,
    non_matched_records.SALESPERSON,
    non_matched_records.ACCOUNT_TYPE,
    non_matched_records.LIEN_PAYOUT,
    non_matched_records.FINANCE_COMMISSION,
    non_matched_records._FIVETRAN_SYNCED
    FROM non_matched_records)



SELECT
    _FILE,
    _LINE,
    _MODIFIED,
    TYPE,
    INVOICE_DATE,
    INVOICE_NUMBER,
    ORDER_NUMBER,
    CUSTOMER_NAME,
    PART_TOTAL,
    LABOR_TOTAL,
    SUBLET_TOTAL,
    FEE_TOTAL,
    SALES_TAX_TOTAL,
    TOTAL,
    OWNER,
    STATUS,
    CREATED,
    MODIFIED,
    UNIT_TOTAL,
    TRADE_IN_TOTAL,
    WARRANTY_PRODUCT_TOTAL,
    OTHER_PRODUCT_TOTAL,
    INVOICE_PROFIT,
    DEAL_PROFIT,
    TAXABLE_TOTAL,
    NON_TAXABLE_TOTAL,
    SALES_COMMISSION,
    SALESPERSON,
    ACCOUNT_TYPE,
    LIEN_PAYOUT,
    FINANCE_COMMISSION,
    _FIVETRAN_SYNCED
FROM updates

UNION

SELECT
    _FILE,
    _LINE,
    _MODIFIED,
    TYPE,
    INVOICE_DATE,
    INVOICE_NUMBER,
    ORDER_NUMBER,
    CUSTOMER_NAME,
    PART_TOTAL,
    LABOR_TOTAL,
    SUBLET_TOTAL,
    FEE_TOTAL,
    SALES_TAX_TOTAL,
    TOTAL,
    OWNER,
    STATUS,
    CREATED,
    MODIFIED,
    UNIT_TOTAL,
    TRADE_IN_TOTAL,
    WARRANTY_PRODUCT_TOTAL,
    OTHER_PRODUCT_TOTAL,
    INVOICE_PROFIT,
    DEAL_PROFIT,
    TAXABLE_TOTAL,
    NON_TAXABLE_TOTAL,
    SALES_COMMISSION,
    SALESPERSON,
    ACCOUNT_TYPE,
    LIEN_PAYOUT,
    FINANCE_COMMISSION,
    _FIVETRAN_SYNCED
FROM inserts