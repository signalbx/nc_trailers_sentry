-- tests if there are any null values in the key columns in the table
SELECT *
FROM {{ source('dp360_prod', 'leads') }}
WHERE NAME IS NULL OR CONTACT IS NULL OR MODIFIED IS NULL