{{
    config (
        materialized = 'view'
    )
}}


WITH src_hosts as (
    SELECT * FROM {{ref("src_hosts")}} 
)
SELECT 
    created_at,
    host_id,
    NVL(host_name, 'Anonymous') AS host_name,
    is_superhost,
    updated_at
FROM  src_hosts
