{{
    config(
        materialized='incremental',
        unique_key='payments_key'
    )
}}
WITH fct_payments AS (
    SELECT *
    FROM {{ ref('int_payments') }}
)
SELECT *
FROM fct_payments