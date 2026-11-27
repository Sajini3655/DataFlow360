select region_key, region_name
from {{ source('warehouse', 'dim_region') }}
