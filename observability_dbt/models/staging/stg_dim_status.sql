SELECT 
    status,
    description,
    message,
    status_key
FROM {{ source('curated', 'dim_status') }}
