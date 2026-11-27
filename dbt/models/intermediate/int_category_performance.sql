select
    category_name,
    region_name,
    date_trunc('month', order_date)::date as sales_month,
    count(*) as line_count,
    sum(quantity) as units_sold,
    sum(revenue) as revenue,
    rank() over (
        partition by date_trunc('month', order_date)
        order by sum(revenue) desc
    ) as monthly_category_rank,
    dense_rank() over (
        partition by region_name order by sum(revenue) desc
    ) as regional_category_rank
from {{ ref('int_sales_enriched') }}
group by category_name, region_name, date_trunc('month', order_date)
