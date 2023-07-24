-- tests if there are any null values in the key columns in the table
SELECT *
FROM {{ source('blackpurl_production', 'invoice_detail_stage') }}
WHERE INVOICE_NUMBER IS NULL OR INVOICE_DATE IS NULL OR ITEM_DESCRIPTION IS NULL