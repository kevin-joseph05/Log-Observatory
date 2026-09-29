SELECT 
    host,
    host_key
FROM {{ source('curated', 'dim_host') }}
