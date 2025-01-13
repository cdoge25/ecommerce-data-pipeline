WITH int_reviews AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['review_id', 'o.order_id', 'product_key']) }} AS review_key,
        o.review_id,
        o.order_id,
        s.product_key,
        o.review_score,
        o.review_comment_title,
        o.review_comment_message,
        o.review_creation_date,
        o.review_answer_timestamp
    FROM {{ ref('stg_order_reviews') }} o
    JOIN {{ ref('fct_sales') }} s
    ON o.order_id = s.order_id
)
SELECT *
FROM int_reviews