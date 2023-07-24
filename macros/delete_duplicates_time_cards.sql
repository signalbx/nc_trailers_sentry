{% macro delete_duplicates_time_cards(schema, table_name, temp_table_name) %}

-- Create a temporary table with distinct values and row numbers
CREATE TEMPORARY TABLE {{ temp_table_name }} AS
SELECT 
    _FILE,
    _LINE,
    _MODIFIED,
    TECHNICIAN,
    TYPE,
    HOURS,
    DATE,
    PERIOD_START,
    _FIVETRAN_SYNCED,
    ROW_NUMBER() OVER (PARTITION BY try_to_date(SUBSTR(_FILE,POSITION('ADP_TIMECARD_' IN UPPER(_FILE)) + LENGTH('ADP_TIMECARD_'), 8), 'YYYYMMDD'), TECHNICIAN, TYPE ORDER BY _MODIFIED DESC) AS row_num
FROM {{ source(schema, table_name) }};

-- Truncate the original table
TRUNCATE TABLE {{ source(schema, table_name) }};

-- Insert the records from the temporary table back into the main table, selecting only the most recent rows
INSERT INTO {{ source(schema, table_name) }} 
        (_FILE,
    _LINE,
    _MODIFIED,
    TECHNICIAN,
    TYPE,
    HOURS,
    DATE,
    PERIOD_START,
    _FIVETRAN_SYNCED)

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
FROM {{ temp_table_name }}
WHERE row_num = 1;

-- Drop the temporary table
DROP TABLE {{ temp_table_name }};

{% endmacro %}