WITH merge_orders_order_payments AS (
    SELECT 
        op.*,
        o.customer_address_id
    FROM {{ ref('stg_order_payments') }} AS op
    LEFT JOIN {{ ref('stg_orders') }} AS o
        ON op.order_id = o.order_id
),

merge_orders_customers AS (
    SELECT 
        m.*,
        c.customer_key
    FROM merge_orders_order_payments AS m
    LEFT JOIN {{ ref('dim_customers') }} AS c
        ON m.customer_address_id = c.customer_address_id
    WHERE c.dbt_valid_to IS NULL
),

int_payments AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['order_id','payment_sequence']) }} AS payments_key,
        *
    FROM merge_orders_customers
)
SELECT *
FROM int_payments