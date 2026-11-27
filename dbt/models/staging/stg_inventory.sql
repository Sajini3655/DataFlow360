select
    inventory_key,
    product_key,
    supplier_key,
    region_key,
    date_key,
    opening_stock,
    quantity_sold,
    closing_stock
from {{ source('warehouse', 'inventory_fact') }}
