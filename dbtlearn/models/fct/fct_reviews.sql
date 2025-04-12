
{{
  config(
    materialized = 'incremental',
    on_schema_change = 'fail' 
  )
}}
-- on_schema_change = 'fail' --> "If someone changes the structure — like adds/removes a column — stop the run and throw an error."
WITH src_reviews AS (
    SELECT * FROM {{ref("src_reviews")}}
)

SELECT  {{ dbt_utils.generate_surrogate_key(['listing_id', 'review_date', 'reviewer_name', 'review_text']) }}
 AS review_id,
 * 
 FROM src_reviews
WHERE review_text IS NOT NULL

-- Incremental filter
-- When a model is incremental, you want to tell dbt: “Only process new or changed rows, not the whole table.”

{% if is_incremental() %}
  AND review_date > (SELECT MAX(review_date) FROM {{ this }}) -- “Only pull rows that are newer than the latest row already in the target table.”
{% endif %}


--{{ this }} is a special dbt variable — it refers to the current model’s table (fct_reviews)