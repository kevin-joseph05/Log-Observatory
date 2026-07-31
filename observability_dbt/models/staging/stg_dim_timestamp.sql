SELECT 
    timestamp,
    day_of_week,
    hour,
    year,
    month,
    day,   
    is_business_hour,   
    period_id,  
    timestamp_key
FROM {{ source('curated', 'dim_timestamp') }}
