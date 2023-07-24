-- tests if there are any null values in the key columns in the table
SELECT *
FROM {{ source('phone_ninjas_prod', 'phone_scores') }}
WHERE NAME IS NULL OR DATE IS NULL OR TOTAL_SCORE IS NULL