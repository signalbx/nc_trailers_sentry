{{ config(
    schema='phone_ninjas_prod',
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['NAME', 'DATE'],
    pre_hook=[
        "{{ delete_null_rows_phone_scores('phone_ninjas_prod', 'phone_scores') }}",
        "{{ delete_duplicates_phone_scores('phone_ninjas_prod', 'phone_scores', 'temp_phone_scores_table') }}",
        "{{ delete_duplicates_phone_scores('blackpurl_production_phone_ninjas_prod', 'phone_scores_prod', 'temp_phone_scores_prod_table') }}",
    ],

    post_hook="TRUNCATE TABLE {{ source('phone_ninjas_prod', 'phone_scores') }};"

) }}


WITH stage AS (
    SELECT 
    _FILE,
    _LINE,
    _MODIFIED,
    COALESCE(TRY_TO_DATE(DATE, 'MM/DD/YYYY'), 
                TRY_TO_DATE(DATE, 'M/D/YYYY'), 
                TRY_TO_DATE(DATE, 'MM-DD-YYYY'), 
                TRY_TO_DATE(DATE, 'M-D-YYYY'), 
                TRY_TO_DATE(DATE, 'YYYY-MM-DD'),
                TRY_TO_DATE(DATE, 'MM/DD/YYYY HH24:MI'),
                TRY_TO_DATE(DATE, 'MM/DD/YYYY HH12:MI AM')) 
        AS formatted_date,
    NAME,
    GREETING,
    QUALIFYING,
    CONTACT_INFO,
    OBJECTIONS,
    CLOSURE,
    DIRECTIONS,
    TOTAL_SCORE,
    _FIVETRAN_SYNCED,
    APPOINTMENT
    FROM {{ source('phone_ninjas_prod', 'phone_scores') }}
    where formatted_date is not null
),

prod AS ( 
    SELECT 
    _FILE,
    _LINE,
    _MODIFIED,
    COALESCE(TRY_TO_DATE(DATE, 'MM/DD/YYYY'), 
                TRY_TO_DATE(DATE, 'M/D/YYYY'), 
                TRY_TO_DATE(DATE, 'MM-DD-YYYY'), 
                TRY_TO_DATE(DATE, 'M-D-YYYY'), 
                TRY_TO_DATE(DATE, 'YYYY-MM-DD'),
                TRY_TO_DATE(DATE, 'MM/DD/YYYY HH24:MI'),
                TRY_TO_DATE(DATE, 'MM/DD/YYYY HH12:MI AM')) 
        AS formatted_date,
    NAME,
    GREETING,
    QUALIFYING,
    CONTACT_INFO,
    OBJECTIONS,
    CLOSURE,
    DIRECTIONS,
    TOTAL_SCORE,
    _FIVETRAN_SYNCED,
    APPOINTMENT
    FROM {{ source('blackpurl_production_phone_ninjas_prod', 'phone_scores_prod') }}
),

matched_records AS (
    SELECT
        stage.*
    FROM stage
    JOIN prod
        ON stage.NAME = prod.NAME
        AND stage.formatted_date = prod.formatted_date
),

updates AS (
    SELECT
    matched_records._FILE,
    matched_records._LINE,
    matched_records._MODIFIED,
    matched_records.formatted_date,
    matched_records.NAME,
    matched_records.GREETING,
    matched_records.QUALIFYING,
    matched_records.CONTACT_INFO,
    matched_records.OBJECTIONS,
    matched_records.CLOSURE,
    matched_records.DIRECTIONS,
    matched_records.TOTAL_SCORE,
    matched_records._FIVETRAN_SYNCED,
    matched_records.APPOINTMENT
    FROM matched_records),

non_matched_records AS (
    SELECT
        stage.*
    FROM stage
    LEFT JOIN prod
        ON stage.NAME = prod.NAME
        AND stage.formatted_date = prod.formatted_date
    WHERE prod.NAME IS NULL AND prod.formatted_date IS NULL
),

inserts AS (
    SELECT
    non_matched_records._FILE,
    non_matched_records._LINE,
    non_matched_records._MODIFIED,
    non_matched_records.formatted_date,
    non_matched_records.NAME,
    non_matched_records.GREETING,
    non_matched_records.QUALIFYING,
    non_matched_records.CONTACT_INFO,
    non_matched_records.OBJECTIONS,
    non_matched_records.CLOSURE,
    non_matched_records.DIRECTIONS,
    non_matched_records.TOTAL_SCORE,
    non_matched_records._FIVETRAN_SYNCED,
    non_matched_records.APPOINTMENT
    FROM non_matched_records)

SELECT
    _FILE,
    _LINE,
    _MODIFIED,
    formatted_date as DATE,
    NAME,
    GREETING,
    QUALIFYING,
    CONTACT_INFO,
    OBJECTIONS,
    CLOSURE,
    DIRECTIONS,
    TOTAL_SCORE,
    _FIVETRAN_SYNCED,
    APPOINTMENT
FROM updates

UNION

SELECT
    _FILE,
    _LINE,
    _MODIFIED,
    formatted_date as DATE,
    NAME,
    GREETING,
    QUALIFYING,
    CONTACT_INFO,
    OBJECTIONS,
    CLOSURE,
    DIRECTIONS,
    TOTAL_SCORE,
    _FIVETRAN_SYNCED,
    APPOINTMENT
FROM inserts