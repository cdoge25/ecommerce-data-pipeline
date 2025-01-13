WITH dim_customers AS (
    SELECT
        DISTINCT customer_id,
    FROM {{ source('landing', 'customers') }}
)

SELECT *
FROM dim_customers