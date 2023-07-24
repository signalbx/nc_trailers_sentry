{% macro delete_duplicates_leads(schema, table_name, temp_table_name) %}

-- Create a temporary table with distinct values and row numbers
CREATE TEMPORARY TABLE {{ temp_table_name }} AS
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
    _FIVETRAN_SYNCED,
    ROW_NUMBER() OVER (PARTITION BY to_timestamp(right(MODIFIED,19), 'MM/DD/YYYY HH12:MI AM')::DATE, NAME, CONTACT ORDER BY _MODIFIED DESC) AS row_num
FROM {{ source(schema, table_name) }};

-- Truncate the original table
TRUNCATE TABLE {{ source(schema, table_name) }};

-- Insert the records from the temporary table back into the main table, selecting only the most recent rows
INSERT INTO {{ source(schema, table_name) }} 
        (_FILE,
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
    _FIVETRAN_SYNCED)

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
FROM {{ temp_table_name }}
WHERE row_num = 1;

-- Drop the temporary table
DROP TABLE {{ temp_table_name }};

{% endmacro %}