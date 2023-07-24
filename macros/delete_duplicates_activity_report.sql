{% macro delete_duplicates_activity_report(schema, table_name, temp_table_name) %}

-- Create a temporary table with distinct values and row numbers
CREATE TEMPORARY TABLE {{ temp_table_name }} AS
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
    _FIVETRAN_SYNCED,
    ROW_NUMBER() OVER (PARTITION BY try_to_date(substr(_FILE, 35, 8), 'YYYYMMDD'), CRM_USER ORDER BY _MODIFIED DESC) AS row_num
FROM {{ source(schema, table_name) }};

-- Truncate the original table
TRUNCATE TABLE {{ source(schema, table_name) }};

-- Insert the records from the temporary table back into the main table, selecting only the most recent rows
INSERT INTO {{ source(schema, table_name) }} 
        (_FILE,
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
    _FIVETRAN_SYNCED)

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
FROM {{ temp_table_name }}
WHERE row_num = 1;

-- Drop the temporary table
DROP TABLE {{ temp_table_name }};

{% endmacro %}