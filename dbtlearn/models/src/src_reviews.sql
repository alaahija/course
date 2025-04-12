WITH raw_reviews as (
select * from {{ source('airbnb', 'reviews') }}

)
SELECT 
    listing_id,
    date AS review_date,
    reviewer_name,
    COMMENTS AS review_text,
    SENTIMENT AS review_sentiment     
FROM
    raw_reviews
