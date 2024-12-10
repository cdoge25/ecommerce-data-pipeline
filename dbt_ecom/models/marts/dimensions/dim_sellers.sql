{{
    config(
        materialized='incremental',
        unique_key='seller_key'
    )
}}
WITH dim_sellers AS (
    SELECT *
    FROM {{ ref('stg_sellers') }}
)
SELECT *
FROM dim_sellers