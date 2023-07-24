{% macro delete_duplicates_numbers(table_name, temp_table_name) %}

-- Create a temporary table with distinct values and row numbers
CREATE TEMPORARY TABLE {{ temp_table_name }} AS
SELECT 
    _FILE,
    _LINE,
    _MODIFIED,
    PHONE_NUMBER,
    CATEGORY,
    NAME,
    LOCATION,
    TYPE,
    NUMBER_TYPE,
    SUBSTITUTE_CALLER_ID_STATUS,
    ASSIGNED_TO,
    EXTENSION_,
    _FIVETRAN_SYNCED,
    ROW_NUMBER() OVER (PARTITION BY PHONE_NUMBER, EXTENSION_ ORDER BY _MODIFIED DESC) AS row_num
FROM {{ source('ring_central_prod', table_name) }};

-- Truncate the original table
TRUNCATE TABLE {{ source('ring_central_prod', table_name) }};

-- Insert the records from the temporary table back into the main table, selecting only the most recent rows
INSERT INTO {{ source('ring_central_prod', table_name) }} 
        (_FILE,
    _LINE,
    _MODIFIED,
    PHONE_NUMBER,
    CATEGORY,
    NAME,
    LOCATION,
    TYPE,
    NUMBER_TYPE,
    SUBSTITUTE_CALLER_ID_STATUS,
    ASSIGNED_TO,
    EXTENSION_,
    _FIVETRAN_SYNCED)

SELECT 
    _FILE,
    _LINE,
    _MODIFIED,
    PHONE_NUMBER,
    CATEGORY,
    NAME,
    LOCATION,
    TYPE,
    NUMBER_TYPE,
    SUBSTITUTE_CALLER_ID_STATUS,
    ASSIGNED_TO,
    EXTENSION_,
    _FIVETRAN_SYNCED
FROM {{ temp_table_name }}
WHERE row_num = 1;

-- Drop the temporary table
DROP TABLE {{ temp_table_name }};

{% endmacro %}