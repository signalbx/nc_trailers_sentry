-- tests if there are any null values in the key columns in the table
SELECT *
FROM {{ source('blackpurl_production', 'deal_units_stage') }}
WHERE STOCK_NUMBER IS NULL OR ORDER_NUMBER IS NULL OR DATE_SOLD IS NULL