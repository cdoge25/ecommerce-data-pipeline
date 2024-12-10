{{
    config(
        materialized='incremental',
        unique_key='sales_key'
    )
}}
WITH fct_sales AS (
    SELECT *
    FROM {{ ref('int_sales') }}
)
SELECT *
FROM fct_sales