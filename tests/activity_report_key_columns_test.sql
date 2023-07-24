-- tests if there are any null values in the key columns in the table
SELECT *
FROM {{ source('dp360_prod', 'activity_report') }}
WHERE _FILE IS NULL OR CRM_USER IS NULL