{% snapshot scd_products %}
{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='product_id',
        strategy='check',
        check_cols=['product_category_name'],
        hard_deletes='invalidate'
    )
}}
SELECT *
FROM {{ source('landing', 'products') }}
{% endsnapshot %}