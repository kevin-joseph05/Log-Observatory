SELECT 
    request_id,
    endpt_key,
    status_key,
    host_key,
    timestamp_key,
    bytes
FROM {{ source('curated', 'fact_requests') }}
