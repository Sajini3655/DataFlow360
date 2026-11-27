select
    region_name,
    category_name,
    count(distinct order_id) as order_count,
    sum(quantity) as units_sold,
    sum(revenue) as revenue,
    rank() over (partition by region_name order by sum(revenue) desc) as category_rank
from {{ ref('fct_sales_line') }}
group by region_name, category_name
