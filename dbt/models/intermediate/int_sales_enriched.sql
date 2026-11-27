with sales as (
    select * from {{ ref('stg_sales') }}
),
enriched as (
    select
        s.order_id,
        s.line_number,
        s.order_date,
        s.customer_id,
        c.customer_key,
        s.customer_name,
        s.product_id,
        p.product_key,
        s.product_name,
        s.category_name,
        s.supplier_id,
        sp.supplier_key,
        s.region_name,
        r.region_key,
        d.date_key,
        s.quantity,
        s.unit_price,
        s.revenue,
        s.source_updated_at
    from sales s
    left join {{ ref('stg_customers') }} c
        on c.customer_id = s.customer_id
    left join {{ ref('stg_products') }} p
        on p.product_name = s.product_name
        and p.category_name = s.category_name
    left join {{ ref('stg_suppliers') }} sp
        on sp.supplier_id = s.supplier_id
    left join {{ ref('stg_regions') }} r
        on r.region_name = s.region_name
    left join {{ source('warehouse', 'dim_date') }} d
        on d.full_date = s.order_date
)
select * from enriched
