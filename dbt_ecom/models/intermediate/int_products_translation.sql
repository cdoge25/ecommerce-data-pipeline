WITH int_products_translation AS (
    SELECT
        p.product_key,
        p.product_id,
        p.product_category_name,
        t.product_category_name_english,
        p.product_name_length,
        p.product_description_length,
        p.product_photos_quantity,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm,
        p.dbt_valid_from,
        p.dbt_valid_to
    FROM {{ ref('stg_products') }} AS p
    LEFT JOIN {{ ref('stg_product_category_name_translation') }} AS t
        ON p.product_category_name = t.product_category_name
)
SELECT *
FROM int_products_translation