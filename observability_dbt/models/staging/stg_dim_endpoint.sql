SELECT 
    method,
    extracted,
    endpt_key,
    endpoint
FROM {{ source('curated', 'dim_endpoint') }}
