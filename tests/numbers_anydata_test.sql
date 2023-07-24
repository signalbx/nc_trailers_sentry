-- tests if any data came over in the nightly data load
WITH source_data AS (
    SELECT COUNT(*) AS row_count
    FROM {{ source('ring_central_prod', 'numbers') }}
)

SELECT row_count
FROM source_data
WHERE row_count = 0