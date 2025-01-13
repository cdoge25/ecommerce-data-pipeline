WITH stg_order_reviews AS (
    SELECT
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp
    FROM {{ ref('int_ranked_reviews') }}
)
SELECT *
FROM stg_order_reviews