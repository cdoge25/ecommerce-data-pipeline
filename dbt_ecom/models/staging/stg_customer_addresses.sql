WITH stg_customer_addresses AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['customer_address_id','dbt_valid_from']) }} as customer_address_key,
        customer_address_id,
        customer_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        dbt_valid_from,
        dbt_valid_to
    FROM {{ ref('scd_customer_addresses') }}
)
SELECT *
FROM stg_customer_addresses