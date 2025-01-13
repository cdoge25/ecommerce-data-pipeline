{{
    config(
        materialized='incremental',
        unique_key='customer_address_key'
    )
}}
WITH dim_customer_addresses AS (
    SELECT *
    FROM {{ ref('stg_customer_addresses') }}
)
SELECT *
FROM dim_customer_addresses