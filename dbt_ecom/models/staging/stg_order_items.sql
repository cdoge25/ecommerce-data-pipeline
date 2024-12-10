WITH stg_order_items AS (
    SELECT *
    FROM {{ source('landing', 'order_items') }}
)
SELECT *
FROM stg_order_items