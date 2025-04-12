WITH 
Listing AS (
    SELECT * FROM {{ref('dim_listings_cleansed')}}
),

hosts AS (
    SELECT * FROM {{ref("dim_hosts_cleansed")}}
)

SELECT
    l.Listing_id,
    l.listing_name,
    l.room_type,
    l.minimum_nights,
    l.price,
    l.host_id,
    h.host_name,
    h.is_superhost as host_is_superhost,
    l.created_at,
    GREATEST(l.updated_at,h.updated_at) as updated_at
FROM Listing L 
LEFT JOIN hosts H ON (l.host_id=l.host_id)