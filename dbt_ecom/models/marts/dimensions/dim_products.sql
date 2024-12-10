{{
    config(
        materialized='incremental',
        unique_key='product_key'
    )
}}
WITH dim_products AS (
    SELECT *
    FROM {{ ref('int_products_translation') }}
)
SELECT *
FROM dim_products