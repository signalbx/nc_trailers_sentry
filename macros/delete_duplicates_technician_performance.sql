{% macro delete_duplicates_technician_performance(schema, table_name, temp_table_name) %}

-- Create a temporary table with distinct values and row numbers
CREATE TEMPORARY TABLE {{ temp_table_name }} AS
SELECT 
    _FILE,
    _LINE,
    _MODIFIED,
    TYPE,
    TECHNICIAN,
    HOURS_WORKED,
    HRS_CLOCKED_ON,
    PRODUCTIVITY,
    CLOCKED_HRS_INVOICED,
    CLOCKED_HRS_WIP,
    INVOICE_HRS,
    INVOICE_HRS_WIP,
    EFFICIENCY,
    PROFICIENCY,
    OTHER_TASKS_HRS,
    INVOICED_COST,
    UNINVOICED_COST,
    COALESCE(TRY_TO_DATE(DATE, 'MM/DD/YYYY'), 
                TRY_TO_DATE(DATE, 'M/D/YYYY'), 
                TRY_TO_DATE(DATE, 'MM-DD-YYYY'), 
                TRY_TO_DATE(DATE, 'M-D-YYYY'), 
                TRY_TO_DATE(DATE, 'YYYY-MM-DD'),
                TRY_TO_DATE(DATE, 'MM/DD/YYYY HH24:MI'),
                TRY_TO_DATE(DATE, 'MM/DD/YYYY HH12:MI AM')) 
        AS formatted_date,
    _FIVETRAN_SYNCED,
    ROW_NUMBER() OVER (PARTITION BY TECHNICIAN, formatted_date ORDER BY _MODIFIED DESC) AS row_num
FROM {{ source(schema, table_name) }};

-- Truncate the original table
TRUNCATE TABLE {{ source(schema, table_name) }};

-- Insert the records from the temporary table back into the main table, selecting only the most recent rows
INSERT INTO {{ source(schema, table_name) }} 
        (_FILE,
    _LINE,
    _MODIFIED,
    TYPE,
    TECHNICIAN,
    HOURS_WORKED,
    HRS_CLOCKED_ON,
    PRODUCTIVITY,
    CLOCKED_HRS_INVOICED,
    CLOCKED_HRS_WIP,
    INVOICE_HRS,
    INVOICE_HRS_WIP,
    EFFICIENCY,
    PROFICIENCY,
    OTHER_TASKS_HRS,
    INVOICED_COST,
    UNINVOICED_COST,
    DATE,
    _FIVETRAN_SYNCED)

SELECT 
    _FILE,
    _LINE,
    _MODIFIED,
    TYPE,
    TECHNICIAN,
    HOURS_WORKED,
    HRS_CLOCKED_ON,
    PRODUCTIVITY,
    CLOCKED_HRS_INVOICED,
    CLOCKED_HRS_WIP,
    INVOICE_HRS,
    INVOICE_HRS_WIP,
    EFFICIENCY,
    PROFICIENCY,
    OTHER_TASKS_HRS,
    INVOICED_COST,
    UNINVOICED_COST,
    formatted_date,
    _FIVETRAN_SYNCED
FROM {{ temp_table_name }}
WHERE row_num = 1;

-- Drop the temporary table
DROP TABLE {{ temp_table_name }};

{% endmacro %}