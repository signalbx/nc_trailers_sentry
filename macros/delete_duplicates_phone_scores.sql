{% macro delete_duplicates_phone_scores(schema, table_name, temp_table_name) %}

-- Create a temporary table with distinct values and row numbers
CREATE TEMPORARY TABLE {{ temp_table_name }} AS
SELECT 
    _FILE,
    _LINE,
    _MODIFIED,
    DATE,
    NAME,
    GREETING,
    QUALIFYING,
    CONTACT_INFO,
    OBJECTIONS,
    CLOSURE,
    DIRECTIONS,
    TOTAL_SCORE,
    _FIVETRAN_SYNCED,
    APPOINTMENT,
    ROW_NUMBER() OVER (PARTITION BY COALESCE(TRY_TO_DATE(DATE, 'MM/DD/YYYY'), 
                TRY_TO_DATE(DATE, 'M/D/YYYY'), 
                TRY_TO_DATE(DATE, 'MM-DD-YYYY'), 
                TRY_TO_DATE(DATE, 'M-D-YYYY'), 
                TRY_TO_DATE(DATE, 'YYYY-MM-DD'),
                TRY_TO_DATE(DATE, 'MM/DD/YYYY HH24:MI'),
                TRY_TO_DATE(DATE, 'MM/DD/YYYY HH12:MI AM')), NAME ORDER BY _MODIFIED DESC) AS row_num
FROM {{ source(schema, table_name) }};

-- Truncate the original table
TRUNCATE TABLE {{ source(schema, table_name) }};

-- Insert the records from the temporary table back into the main table, selecting only the most recent rows
INSERT INTO {{ source(schema, table_name) }} 
        (_FILE,
    _LINE,
    _MODIFIED,
    DATE,
    NAME,
    GREETING,
    QUALIFYING,
    CONTACT_INFO,
    OBJECTIONS,
    CLOSURE,
    DIRECTIONS,
    TOTAL_SCORE,
    _FIVETRAN_SYNCED,
    APPOINTMENT)

SELECT 
    _FILE,
    _LINE,
    _MODIFIED,
    DATE,
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
FROM {{ temp_table_name }}
WHERE row_num = 1;

-- Drop the temporary table
DROP TABLE {{ temp_table_name }};

{% endmacro %}