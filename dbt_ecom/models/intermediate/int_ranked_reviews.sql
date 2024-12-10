WITH int_ranked_reviews AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY review_creation_date DESC) AS rn
    FROM {{ source('landing', 'order_reviews') }}
)
SELECT *
FROM int_ranked_reviews
WHERE rn = 1