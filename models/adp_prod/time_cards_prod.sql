--This is a pipeline that extracts date from the file name but has a column in the table for inserting the date (Pre-hook to insert the date is required)
{{ config(
    schema='adp_prod',
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['TECHNICIAN', 'TYPE', 'PERIOD_START'],
    pre_hook=[
        "{{ delete_duplicates_time_cards('adp_prod', 'time_cards', 'temp_time_cards_table') }}",
        "{{ delete_duplicates_time_cards('blackpurl_production_adp_prod', 'time_cards_prod', 'temp_time_cards_prod_table') }}",
        "{{ file_name_date_time_cards('adp_prod', 'time_cards') }}",
        "{{ file_name_date_time_cards('blackpurl_production_adp_prod', 'time_cards_prod') }}",
    ],

    post_hook="TRUNCATE TABLE {{ source('adp_prod', 'time_cards') }};"

) }}


WITH stage AS (
    SELECT 
    _FILE,
    _LINE,
    _MODIFIED,
    TECHNICIAN,
    TYPE,
    HOURS,
    DATE,
    PERIOD_START,
    _FIVETRAN_SYNCED
    FROM {{ source('adp_prod', 'time_cards') }}
    where PERIOD_START is not null
),

prod AS ( 
    SELECT 
    _FILE,
    _LINE,
    _MODIFIED,
    TECHNICIAN,
    TYPE,
    HOURS,
    DATE,
    PERIOD_START,
    _FIVETRAN_SYNCED
    FROM {{ source('blackpurl_production_adp_prod', 'time_cards_prod') }}
    where PERIOD_START is not null
),

matched_records AS (
    SELECT
        stage.*
    FROM stage
    JOIN prod
        ON stage.TECHNICIAN = prod.TECHNICIAN
        AND stage.TYPE = prod.TYPE
        AND stage.PERIOD_START = prod.PERIOD_START
),

updates AS (
    SELECT
    matched_records._FILE,
    matched_records._LINE,
    matched_records._MODIFIED,
    matched_records.TECHNICIAN,
    matched_records.TYPE,
    matched_records.HOURS,
    matched_records.DATE,
    matched_records.PERIOD_START,
    matched_records._FIVETRAN_SYNCED
    FROM matched_records),

non_matched_records AS (
    SELECT
        stage.*
    FROM stage
    LEFT JOIN prod
        ON stage.TECHNICIAN = prod.TECHNICIAN
        AND stage.TYPE = prod.TYPE
        AND stage.PERIOD_START = prod.PERIOD_START
    WHERE prod.TECHNICIAN IS NULL AND prod.TYPE IS NULL AND prod.PERIOD_START IS NULL
),

inserts AS (
    SELECT
    non_matched_records._FILE,
    non_matched_records._LINE,
    non_matched_records._MODIFIED,
    non_matched_records.TECHNICIAN,
    non_matched_records.TYPE,
    non_matched_records.HOURS,
    non_matched_records.DATE,
    non_matched_records.PERIOD_START,
    non_matched_records._FIVETRAN_SYNCED
    FROM non_matched_records)

SELECT
    _FILE,
    _LINE,
    _MODIFIED,
    TECHNICIAN,
    TYPE,
    HOURS,
    DATE,
    PERIOD_START,
    _FIVETRAN_SYNCED
FROM updates

UNION

SELECT
    _FILE,
    _LINE,
    _MODIFIED,
    TECHNICIAN,
    TYPE,
    HOURS,
    DATE,
    PERIOD_START,
    _FIVETRAN_SYNCED
FROM inserts