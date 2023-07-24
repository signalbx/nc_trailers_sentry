-- tests if there was any data in the call_log table for the current date

WITH source_data AS (
    SELECT COUNT(*) AS row_count
    FROM {{ source('ring_central_prod', 'call_log') }}
    WHERE ifnull(try_to_date("DATE", 'YYYY-MM-DD'), try_to_date(right("DATE",10), 'MM/DD/YYYY'))::date = (CURRENT_DATE - INTERVAL '1 DAY')
)

SELECT 1
WHERE NOT EXISTS (
    SELECT 1
    FROM source_data
    WHERE row_count > 0
)
