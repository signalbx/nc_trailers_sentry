-- tests if there were any rows for yesterday in the nightly data load
WITH source_data AS (
    SELECT COUNT(*) AS row_count
    FROM {{ source('google_sheets', 'sales_spreadsheet') }}
    WHERE to_timestamp(_FIVETRAN_SYNCED)::date = (current_date - interval '1 day')
)

SELECT row_count
FROM source_data
WHERE row_count = 0