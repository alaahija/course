WITH raw_hosts as (
select * from {{ source('airbnb', 'hosts') }}

)
SELECT 
    CREATED_AT,
    ID AS host_id,
    NAME AS host_name,
    IS_SUPERHOST,
    UPDATED_AT    
FROM
    raw_hosts
