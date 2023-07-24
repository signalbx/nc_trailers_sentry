--This is a pipeline that extracts date from the modified date column (No Unique Key variable in Config and deduplication is required in cte)
{{ config(
    schema='DP360_PROD',
    materialized='incremental',
    incremental_strategy='merge',
    pre_hook=[
        "{{ delete_duplicates_leads('dp360_prod', 'leads', 'temp_leads_table') }}",
        "{{ delete_duplicates_leads('blackpurl_production_dp360_prod', 'leads_prod', 'temp_leads_prod_table') }}",
    ],

    post_hook="TRUNCATE TABLE {{ source('dp360_prod', 'leads') }};"

) }}


WITH stage AS (
    SELECT 
    _FILE,
    _LINE,
    _MODIFIED,
    NAME,
    CONTACT,
    CREATED,
    MODIFIED,
    to_timestamp(right(MODIFIED,19), 'MM/DD/YYYY HH12:MI AM')::DATE as formatted_date,
    VEHICLE,
    COMPANY_NAME,
    FORMS,
    LEAD_DETAILS,
    STEP,
    TAGS,
    LOCATION,
    SALES_PERSON,
    ORIGINATOR,
    DNC_STATUS,
    LATEST_NOTE,
    _FIVETRAN_SYNCED
    FROM {{ source('dp360_prod', 'leads') }}
),

prod AS ( 
    SELECT 
    _FILE,
    _LINE,
    _MODIFIED,
    NAME,
    CONTACT,
    CREATED,
    MODIFIED,
    to_timestamp(right(MODIFIED,19), 'MM/DD/YYYY HH12:MI AM')::DATE as formatted_date,
    VEHICLE,
    COMPANY_NAME,
    FORMS,
    LEAD_DETAILS,
    STEP,
    TAGS,
    LOCATION,
    SALES_PERSON,
    ORIGINATOR,
    DNC_STATUS,
    LATEST_NOTE,
    _FIVETRAN_SYNCED
    FROM {{ source('blackpurl_production_dp360_prod', 'leads_prod') }}
),

matched_records AS (
    SELECT
        stage.*
    FROM stage
    JOIN prod
        ON stage.NAME = prod.NAME
        AND stage.formatted_date = prod.formatted_date
        AND stage.CONTACT = prod.CONTACT
),

updates AS (
    SELECT
    matched_records._FILE,
    matched_records._LINE,
    matched_records._MODIFIED,
    matched_records.NAME,
    matched_records.CONTACT,
    matched_records.CREATED,
    matched_records.MODIFIED,
    matched_records.formatted_date,
    matched_records.VEHICLE,
    matched_records.COMPANY_NAME,
    matched_records.FORMS,
    matched_records.LEAD_DETAILS,
    matched_records.STEP,
    matched_records.TAGS,
    matched_records.LOCATION,
    matched_records.SALES_PERSON,
    matched_records.ORIGINATOR,
    matched_records.DNC_STATUS,
    matched_records.LATEST_NOTE,
    matched_records._FIVETRAN_SYNCED
    FROM matched_records),

non_matched_records AS (
    SELECT
        stage.*
    FROM stage
    LEFT JOIN prod
        ON stage.NAME = prod.NAME
        AND stage.formatted_date = prod.formatted_date
        AND stage.CONTACT = prod.CONTACT
    WHERE prod.NAME IS NULL AND prod.formatted_date IS NULL AND prod.CONTACT IS NULL
),

inserts AS (
    SELECT
    non_matched_records._FILE,
    non_matched_records._LINE,
    non_matched_records._MODIFIED,
    non_matched_records.NAME,
    non_matched_records.CONTACT,
    non_matched_records.CREATED,
    non_matched_records.MODIFIED,
    non_matched_records.formatted_date,
    non_matched_records.VEHICLE,
    non_matched_records.COMPANY_NAME,
    non_matched_records.FORMS,
    non_matched_records.LEAD_DETAILS,
    non_matched_records.STEP,
    non_matched_records.TAGS,
    non_matched_records.LOCATION,
    non_matched_records.SALES_PERSON,
    non_matched_records.ORIGINATOR,
    non_matched_records.DNC_STATUS,
    non_matched_records.LATEST_NOTE,
    non_matched_records._FIVETRAN_SYNCED
    FROM non_matched_records),

deduped_updates AS (
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY formatted_date, NAME, CONTACT ORDER BY _MODIFIED DESC) as row_number
    FROM updates
),

deduped_inserts AS (
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY formatted_date, NAME, CONTACT ORDER BY _MODIFIED DESC) as row_number
    FROM inserts
),

final_updates AS (
    SELECT
        _FILE,
        _LINE,
        _MODIFIED,
        NAME,
        CONTACT,
        CREATED,
        MODIFIED,
        VEHICLE,
        COMPANY_NAME,
        FORMS,
        LEAD_DETAILS,
        STEP,
        TAGS,
        LOCATION,
        SALES_PERSON,
        ORIGINATOR,
        DNC_STATUS,
        LATEST_NOTE,
        _FIVETRAN_SYNCED
    FROM deduped_updates
    WHERE row_number = 1
),

final_inserts AS (
    SELECT
        _FILE,
        _LINE,
        _MODIFIED,
        NAME,
        CONTACT,
        CREATED,
        MODIFIED,
        VEHICLE,
        COMPANY_NAME,
        FORMS,
        LEAD_DETAILS,
        STEP,
        TAGS,
        LOCATION,
        SALES_PERSON,
        ORIGINATOR,
        DNC_STATUS,
        LATEST_NOTE,
        _FIVETRAN_SYNCED
    FROM deduped_inserts
    WHERE row_number = 1
)

SELECT * FROM final_updates
UNION
SELECT * FROM final_inserts