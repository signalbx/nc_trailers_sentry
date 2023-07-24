-- tests if there are any null values in the key columns in the table
SELECT *
FROM {{ source('blackpurl_production', 'customers_stage') }}
WHERE CUSTOMER_NUMBER IS NULL