WITH stg_orders AS (
    SELECT
        order_id,
        customer_address_id,
        order_status,
        order_purchase_timestamp,
        order_approved_timestamp,
        order_pickup_timestamp,
        order_delivered_timestamp,
        order_estimated_delivery_date,
        CAST(TO_CHAR(order_purchase_timestamp, 'YYYYMMDD') AS INT) AS order_purchase_date_key,
        CAST(TO_CHAR(order_approved_timestamp, 'YYYYMMDD') AS INT) AS order_approved_date_key,
        CAST(TO_CHAR(order_pickup_timestamp, 'YYYYMMDD') AS INT) AS order_pickup_date_key,
        CAST(TO_CHAR(order_delivered_timestamp, 'YYYYMMDD') AS INT) AS order_delivered_date_key,
        EXTRACT(HOUR, order_purchase_timestamp) AS order_purchase_time_key,
        EXTRACT(HOUR, order_approved_timestamp) AS order_approved_time_key,
        EXTRACT(HOUR, order_pickup_timestamp) AS order_pickup_time_key,
        EXTRACT(HOUR, order_delivered_timestamp) AS order_delivered_time_key
    FROM {{ source('landing', 'orders') }}
)
SELECT *
FROM stg_orders