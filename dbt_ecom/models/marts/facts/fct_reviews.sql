{{
    config(
        materialized='incremental',
        unique_key='review_key'
    )
}}
WITH fct_reviews AS (
    SELECT DISTINCT *
    FROM {{ ref('int_reviews') }}
)
SELECT *
FROM fct_reviews
