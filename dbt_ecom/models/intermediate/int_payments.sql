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
        ca.customer_id
    FROM merge_orders_order_payments AS m
    LEFT JOIN {{ ref('dim_customer_addresses') }} AS ca
        ON m.customer_address_id = ca.customer_address_id
    WHERE ca.dbt_valid_to IS NULL
),

int_payments AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['order_id','payment_sequence']) }} AS payments_key,
        order_id,
        payment_sequence,
        payment_type,
        payment_installments,
        payment_value,
        customer_id
    FROM merge_orders_customers
)
SELECT *
FROM int_payments