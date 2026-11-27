select
    customer_key,
    customer_id,
    customer_name,
    region as region_name
from {{ source('warehouse', 'dim_customer') }}
