-- tests if there were any rows for yesterday in the nightly data load
WITH source_data AS (
    SELECT COUNT(*) AS row_count
    FROM {{ source('facebook_ads', 'basic_all_levels') }}
    WHERE to_timestamp(_FIVETRAN_SYNCED)::date = (current_date)
)

SELECT row_count
FROM source_data
WHERE row_count = 0