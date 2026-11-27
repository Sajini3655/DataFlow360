{{
    config(
        materialized='incremental',
        unique_key=['order_id', 'line_number'],
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns'
    )
}}

select
    order_id,
    line_number,
    order_date,
    customer_id,
    customer_name,
    customer_key,
    product_id,
    product_key,
    product_name,
    category_name,
    supplier_id,
    supplier_key,
    region_name,
    region_key,
    date_key,
    quantity,
    unit_price,
    revenue,
    source_updated_at
from {{ ref('int_sales_enriched') }}
{% if is_incremental() %}
where source_updated_at >= (
    select coalesce(max(source_updated_at), '1900-01-01'::timestamptz)
    from {{ this }}
)
{% endif %}
