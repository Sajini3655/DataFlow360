select
    product_id,
    product_name,
    category_name,
    count(distinct order_id) as order_count,
    sum(quantity) as units_sold,
    sum(revenue) as revenue,
    avg(unit_price) as average_unit_price
from {{ ref('fct_sales_line') }}
group by product_id, product_name, category_name
