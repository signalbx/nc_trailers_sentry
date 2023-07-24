-- tests if there are any null values in the key columns in the table
SELECT *
FROM {{ source('blackpurl_production', 'technician_performance_stage') }}
WHERE TECHNICIAN IS NULL OR DATE IS NULL