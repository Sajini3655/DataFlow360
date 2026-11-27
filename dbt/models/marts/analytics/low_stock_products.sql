select
    product_name,
    category_name,
    supplier_name,
    region_name,
    snapshot_date,
    closing_stock
from {{ ref('int_inventory_movements') }}
where closing_stock <= 25
