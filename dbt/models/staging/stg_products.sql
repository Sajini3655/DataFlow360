select
    p.product_key,
    p.product_name,
    c.category_name
from {{ source('warehouse', 'dim_product') }} p
join {{ source('warehouse', 'dim_category') }} c
    on c.category_key = p.category_key
