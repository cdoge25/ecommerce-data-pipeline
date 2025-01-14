WITH merge_orders_order_items AS (
    SELECT
        o.*,
        oi.order_product_sequence,
        oi.product_id,
        oi.seller_id,
        oi.order_limit_delivery_timestamp,
        oi.price,
        oi.freight_value
    FROM {{ ref('stg_order_items') }} AS oi
    LEFT JOIN {{ ref('stg_orders') }} AS o
        ON oi.order_id = o.order_id
),

merge_orders_customer_addresses AS (
    SELECT 
        m.*,
        ca.customer_address_key,
        ca.customer_id
    FROM merge_orders_order_items AS m
    LEFT JOIN {{ ref('dim_customer_addresses') }} AS ca
        ON m.customer_address_id = ca.customer_address_id
    WHERE ca.dbt_valid_to IS NULL
),

merge_orders_sellers AS (
    SELECT
        m.*,
        s.seller_key
    FROM merge_orders_customer_addresses AS m
    LEFT JOIN {{ ref('dim_sellers') }} AS s
        ON m.seller_id = s.seller_id
    WHERE s.dbt_valid_to IS NULL
),

merge_orders_products AS (
    SELECT
        m.*,
        p.product_key
    FROM merge_orders_sellers AS m
    LEFT JOIN {{ ref('dim_products') }} AS p
        ON m.product_id = p.product_id
),

int_sales AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['order_id','order_product_sequence']) }} AS sales_key,
        order_id,
        order_product_sequence,
        product_key,
        customer_address_key,
        customer_id,
        seller_key,
        price,
        freight_value,
        order_status,
        order_purchase_timestamp,
        order_approved_timestamp,
        order_pickup_timestamp,
        order_delivered_timestamp,
        order_limit_delivery_timestamp,
        order_estimated_delivery_date,
        order_purchase_date_key,
        order_approved_date_key,
        order_pickup_date_key,
        order_delivered_date_key,
        order_purchase_time_key,
        order_approved_time_key,
        order_pickup_time_key,
        order_delivered_time_key
    FROM merge_orders_products
)
SELECT * FROM int_sales