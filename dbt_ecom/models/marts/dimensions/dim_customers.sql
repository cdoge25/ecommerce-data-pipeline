{{
    config(
        materialized='incremental',
        unique_key='customer_key'
    )
}}
WITH dim_customers AS (
    SELECT *
    FROM {{ ref('stg_customers') }}
)
SELECT *
FROM dim_customers