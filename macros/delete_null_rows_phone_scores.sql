{% macro delete_null_rows_phone_scores(schema, table_name) %}

DELETE FROM {{ source(schema, table_name) }}
WHERE DATE IS NULL AND NAME IS NULL;

{% endmacro %}