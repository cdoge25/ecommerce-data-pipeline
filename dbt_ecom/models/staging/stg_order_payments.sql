WITH stg_order_payments AS (
    SELECT *
    FROM {{ source('landing', 'order_payments') }}
)
SELECT *
FROM stg_order_payments