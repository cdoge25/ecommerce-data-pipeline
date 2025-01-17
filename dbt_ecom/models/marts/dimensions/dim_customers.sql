{{
    config(
        materialized='incremental',
        unique_key='customer_id'
    )
}}
WITH dim_customers AS (
    SELECT
        DISTINCT customer_id,
    FROM {{ source('landing', 'customers') }}
)

SELECT *
FROM dim_customers