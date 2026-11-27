select
    i.inventory_key,
    p.product_name,
    c.category_name,
    s.supplier_name,
    r.region_name,
    d.full_date as snapshot_date,
    i.opening_stock,
    i.quantity_sold,
    i.closing_stock,
    case
        when i.opening_stock > 0
        then i.quantity_sold::numeric / i.opening_stock
        else 0
    end as inventory_turnover
from {{ ref('stg_inventory') }} i
join {{ ref('stg_products') }} p on p.product_key = i.product_key
join {{ ref('stg_suppliers') }} s on s.supplier_key = i.supplier_key
join {{ ref('stg_regions') }} r on r.region_key = i.region_key
join {{ source('warehouse', 'dim_date') }} d on d.date_key = i.date_key
join {{ source('warehouse', 'dim_category') }} c
    on c.category_name = p.category_name
