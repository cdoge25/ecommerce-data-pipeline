{{
    config(
        materialized='incremental',
        unique_key='review_key'
    )
}}
WITH dim_reviews AS (
    SELECT *
    FROM {{ ref('stg_order_reviews') }}
)
SELECT *
FROM dim_reviews