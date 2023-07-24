-- tests if the fivetran data load ran successfully

WITH source_data AS (
    SELECT COUNT(*) AS row_count
    FROM {{ source('ring_central_prod', 'numbers_stage') }}
    WHERE to_timestamp(_FIVETRAN_SYNCED)::date = (CURRENT_DATE - INTERVAL '1 DAY')
)

SELECT 1
WHERE NOT EXISTS (
    SELECT 1
    FROM source_data
    WHERE row_count > 0
)