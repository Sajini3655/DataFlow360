select
    order_date,
    region_name,
    category_name,
    count(*) as line_count,
    count(distinct order_id) as order_count,
    sum(quantity) as units_sold,
    sum(revenue) as revenue,
    avg(revenue) as average_line_revenue
from {{ ref('fct_sales_line') }}
group by order_date, region_name, category_name
