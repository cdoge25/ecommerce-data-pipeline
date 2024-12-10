WITH stg_customers AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['customer_address_id','dbt_valid_from']) }} as customer_key,
        customer_address_id,
        customer_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        dbt_valid_from,
        dbt_valid_to
    FROM {{ ref('scd_customers') }}
)
SELECT *
FROM stg_customers