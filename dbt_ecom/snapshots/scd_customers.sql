{% snapshot scd_customers %}
{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='customer_address_id',
        strategy='check',
        check_cols=['customer_zip_code_prefix','customer_city','customer_state'],
        hard_deletes='invalidate'
    )
}}
SELECT *
FROM {{ source('landing', 'customers') }}
{% endsnapshot %}