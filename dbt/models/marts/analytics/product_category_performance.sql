select
    category_name,
    product_name,
    sum(quantity) as units_sold,
    sum(revenue) as revenue,
    dense_rank() over (
        partition by category_name order by sum(revenue) desc
    ) as category_product_rank
from {{ ref('fct_sales_line') }}
group by category_name, product_name
