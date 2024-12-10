WITH stg_products AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['product_id','dbt_valid_from']) }} as product_key,
        product_id,
        product_category_name,
        product_name_length,
        product_description_length,
        product_photos_quantity,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        dbt_valid_from,
        dbt_valid_to
    FROM {{ ref('scd_products') }}
)
SELECT *
FROM stg_products