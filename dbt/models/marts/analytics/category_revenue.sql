select
    category_name,
    count(distinct order_id) as order_count,
    sum(quantity) as units_sold,
    sum(revenue) as revenue,
    sum(revenue) / nullif(sum(sum(revenue)) over (), 0) as revenue_share
from {{ ref('fct_sales_line') }}
group by category_name
