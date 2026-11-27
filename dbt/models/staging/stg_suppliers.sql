select
    supplier_key,
    supplier_id,
    supplier_name,
    region as region_name
from {{ source('warehouse', 'dim_supplier') }}
