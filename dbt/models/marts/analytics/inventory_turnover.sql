select
    product_name,
    category_name,
    supplier_name,
    region_name,
    snapshot_date,
    opening_stock,
    quantity_sold,
    closing_stock,
    inventory_turnover
from {{ ref('int_inventory_movements') }}
