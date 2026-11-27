with source_total as (
    select coalesce(sum(revenue), 0) as revenue
    from {{ ref('stg_sales') }}
),
mart_total as (
    select coalesce(sum(revenue), 0) as revenue
    from {{ ref('fct_sales_line') }}
)
select source_total.revenue as source_revenue, mart_total.revenue as mart_revenue
from source_total
cross join mart_total
where abs(source_total.revenue - mart_total.revenue) > 0.01
