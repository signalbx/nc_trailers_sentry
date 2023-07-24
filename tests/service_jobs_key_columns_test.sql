-- tests if there are any null values in the key columns in the table
SELECT *
FROM {{ source('blackpurl_production', 'service_jobs_stage') }}
WHERE ORDER_NUMBER IS NULL OR INVOICE_DATE IS NULL OR JOB IS NULL OR TECHNICIAN IS NULL