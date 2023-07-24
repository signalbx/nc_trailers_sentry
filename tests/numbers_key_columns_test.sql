-- tests if there are any null values in the key columns in the table
SELECT *
FROM {{ source('ring_central_prod', 'numbers') }}
WHERE PHONE_NUMBER IS NULL OR EXTENSION_ IS NULL OR TYPE IS NULL