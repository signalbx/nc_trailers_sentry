{% macro file_name_date_time_cards(schema, table_name) %}

UPDATE {{ source(schema, table_name) }}
SET PERIOD_START = try_to_date(SUBSTR(_FILE,POSITION('ADP_TIMECARD_' IN UPPER(_FILE)) + LENGTH('ADP_TIMECARD_'), 8), 'YYYYMMDD')
WHERE PERIOD_START is null;

{% endmacro %}