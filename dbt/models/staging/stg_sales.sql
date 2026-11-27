select
    order_id::text as order_id,
    line_number::integer as line_number,
    customer_id::text as customer_id,
    customer_name::text as customer_name,
    product_id::text as product_id,
    product::text as product_name,
    category::text as category_name,
    supplier_id::text as supplier_id,
    region::text as region_name,
    order_date::date as order_date,
    quantity::integer as quantity,
    unit_price::numeric(12, 2) as unit_price,
    revenue::numeric(14, 2) as revenue,
    transformed_at::timestamptz as source_updated_at
from {{ source('warehouse', 'silver_sales') }}
