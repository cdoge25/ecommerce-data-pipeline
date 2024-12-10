{% snapshot scd_sellers %}
{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='seller_id',
        strategy='check',
        check_cols=['seller_zip_code_prefix','seller_city','seller_state'],
        hard_deletes='invalidate'
    )
}}
SELECT *
FROM {{ source('landing', 'sellers') }}
{% endsnapshot %}