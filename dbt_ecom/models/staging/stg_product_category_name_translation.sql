WITH stg_product_category_name_translation AS (
    SELECT *
    FROM {{ source('landing', 'product_category_name_translation') }}
)
SELECT *
FROM stg_product_category_name_translation