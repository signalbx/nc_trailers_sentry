-- tests if there are any null values in the key columns in the table
SELECT *
FROM {{ source('adp_prod', 'time_cards') }}
WHERE TECHNICIAN IS NULL OR _FILE IS NULL OR TYPE IS NULL