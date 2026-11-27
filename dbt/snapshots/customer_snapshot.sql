{% snapshot customer_snapshot %}
{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['customer_name', 'region_name']
    )
}}

select customer_id, customer_name, region_name
from {{ ref('stg_customers') }}
{% endsnapshot %}
