--This is a pipeline that extracts date from the file name (No Unique Key variable in Config and deduplication is required in cte)
{{ config(
    schema='DP360_PROD',
    materialized='incremental',
    incremental_strategy='merge',
    pre_hook=[
        "{{ delete_duplicates_activity_report('dp360_prod', 'activity_report', 'temp_activity_report_table') }}",
        "{{ delete_duplicates_activity_report('blackpurl_production_dp360_prod', 'activity_report_prod', 'temp_activity_report_prod_table') }}",
    ],

    post_hook="TRUNCATE TABLE {{ source('dp360_prod', 'activity_report') }};"

) }}


WITH stage AS (
    SELECT 
    _FILE,
    _LINE,
    _MODIFIED,
    CRM_USER,
    try_to_date(substr(_FILE, 35, 8), 'YYYYMMDD') as formatted_date,
    LEAD_ACTIVITIES,
    INBOUND_PHONE_LEADS,
    OUTBOUND_CALLS,
    OUTBOUND_CONTACTED,
    SMS_SENT,
    EMAILS_SENT,
    SHOWROOM_LEADS,
    DEALER_VISITS,
    APPTS_SET,
    APPTS_SHOWS,
    APPTS_SOLD,
    APPTS_SOLD_,
    WEB_LEADS_ASSIGNED,
    WEB_LEADS_WORKED,
    UNWORKED_LEADS,
    OVERDUE_EVENTS,
    OVERALL_SOLD,
    _FIVETRAN_SYNCED
    FROM {{ source('dp360_prod', 'activity_report') }}
    where formatted_date is not null
),

prod AS ( 
    SELECT 
    _FILE,
    _LINE,
    _MODIFIED,
    CRM_USER,
    try_to_date(substr(_FILE, 35, 8), 'YYYYMMDD') as formatted_date,
    LEAD_ACTIVITIES,
    INBOUND_PHONE_LEADS,
    OUTBOUND_CALLS,
    OUTBOUND_CONTACTED,
    SMS_SENT,
    EMAILS_SENT,
    SHOWROOM_LEADS,
    DEALER_VISITS,
    APPTS_SET,
    APPTS_SHOWS,
    APPTS_SOLD,
    APPTS_SOLD_,
    WEB_LEADS_ASSIGNED,
    WEB_LEADS_WORKED,
    UNWORKED_LEADS,
    OVERDUE_EVENTS,
    OVERALL_SOLD,
    _FIVETRAN_SYNCED
    FROM {{ source('blackpurl_production_dp360_prod', 'activity_report_prod') }}
),

matched_records AS (
    SELECT
        stage.*
    FROM stage
    JOIN prod
        ON stage.CRM_USER = prod.CRM_USER
        AND stage.formatted_date = prod.formatted_date
),

updates AS (
    SELECT
    matched_records._FILE,
    matched_records._LINE,
    matched_records._MODIFIED,
    matched_records.CRM_USER,
    matched_records.formatted_date,
    matched_records.LEAD_ACTIVITIES,
    matched_records.INBOUND_PHONE_LEADS,
    matched_records.OUTBOUND_CALLS,
    matched_records.OUTBOUND_CONTACTED,
    matched_records.SMS_SENT,
    matched_records.EMAILS_SENT,
    matched_records.SHOWROOM_LEADS,
    matched_records.DEALER_VISITS,
    matched_records.APPTS_SET,
    matched_records.APPTS_SHOWS,
    matched_records.APPTS_SOLD,
    matched_records.APPTS_SOLD_,
    matched_records.WEB_LEADS_ASSIGNED,
    matched_records.WEB_LEADS_WORKED,
    matched_records.UNWORKED_LEADS,
    matched_records.OVERDUE_EVENTS,
    matched_records.OVERALL_SOLD,
    matched_records._FIVETRAN_SYNCED
    FROM matched_records),

non_matched_records AS (
    SELECT
        stage.*
    FROM stage
    LEFT JOIN prod
        ON stage.CRM_USER = prod.CRM_USER
        AND stage.formatted_date = prod.formatted_date
    WHERE prod.CRM_USER IS NULL AND prod.formatted_date IS NULL
),

inserts AS (
    SELECT
    non_matched_records._FILE,
    non_matched_records._LINE,
    non_matched_records._MODIFIED,
    non_matched_records.CRM_USER,
    non_matched_records.formatted_date,
    non_matched_records.LEAD_ACTIVITIES,
    non_matched_records.INBOUND_PHONE_LEADS,
    non_matched_records.OUTBOUND_CALLS,
    non_matched_records.OUTBOUND_CONTACTED,
    non_matched_records.SMS_SENT,
    non_matched_records.EMAILS_SENT,
    non_matched_records.SHOWROOM_LEADS,
    non_matched_records.DEALER_VISITS,
    non_matched_records.APPTS_SET,
    non_matched_records.APPTS_SHOWS,
    non_matched_records.APPTS_SOLD,
    non_matched_records.APPTS_SOLD_,
    non_matched_records.WEB_LEADS_ASSIGNED,
    non_matched_records.WEB_LEADS_WORKED,
    non_matched_records.UNWORKED_LEADS,
    non_matched_records.OVERDUE_EVENTS,
    non_matched_records.OVERALL_SOLD,
    non_matched_records._FIVETRAN_SYNCED
    FROM non_matched_records),

deduped_updates AS (
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY formatted_date, CRM_USER ORDER BY _MODIFIED DESC) as row_number
    FROM updates
),

deduped_inserts AS (
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY formatted_date, CRM_USER ORDER BY _MODIFIED DESC) as row_number
    FROM inserts
),

final_updates AS (
    SELECT
        _FILE,
        _LINE,
        _MODIFIED,
        CRM_USER,
        LEAD_ACTIVITIES,
        INBOUND_PHONE_LEADS,
        OUTBOUND_CALLS,
        OUTBOUND_CONTACTED,
        SMS_SENT,
        EMAILS_SENT,
        SHOWROOM_LEADS,
        DEALER_VISITS,
        APPTS_SET,
        APPTS_SHOWS,
        APPTS_SOLD,
        APPTS_SOLD_,
        WEB_LEADS_ASSIGNED,
        WEB_LEADS_WORKED,
        UNWORKED_LEADS,
        OVERDUE_EVENTS,
        OVERALL_SOLD,
        _FIVETRAN_SYNCED
    FROM deduped_updates
    WHERE row_number = 1
),

final_inserts AS (
    SELECT
        _FILE,
        _LINE,
        _MODIFIED,
        CRM_USER,
        LEAD_ACTIVITIES,
        INBOUND_PHONE_LEADS,
        OUTBOUND_CALLS,
        OUTBOUND_CONTACTED,
        SMS_SENT,
        EMAILS_SENT,
        SHOWROOM_LEADS,
        DEALER_VISITS,
        APPTS_SET,
        APPTS_SHOWS,
        APPTS_SOLD,
        APPTS_SOLD_,
        WEB_LEADS_ASSIGNED,
        WEB_LEADS_WORKED,
        UNWORKED_LEADS,
        OVERDUE_EVENTS,
        OVERALL_SOLD,
        _FIVETRAN_SYNCED
    FROM deduped_inserts
    WHERE row_number = 1
)

SELECT * FROM final_updates
UNION
SELECT * FROM final_inserts