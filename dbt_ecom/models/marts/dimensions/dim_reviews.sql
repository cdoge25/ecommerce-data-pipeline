{{
    config(
        materialized='incremental',
        unique_key='review_key'
    )
}}
WITH dim_reviews AS (
    SELECT
        review_key,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp,
    FROM {{ ref('stg_order_reviews') }}
)
SELECT *
FROM dim_reviews
