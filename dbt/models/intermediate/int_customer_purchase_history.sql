select
    customer_id,
    order_id,
    line_number,
    order_date,
    revenue,
    row_number() over (
        partition by customer_id order by order_date, order_id, line_number
    ) as purchase_number,
    sum(revenue) over (
        partition by customer_id order by order_date, order_id, line_number
        rows between unbounded preceding and current row
    ) as customer_running_revenue,
    lag(order_date) over (
        partition by customer_id order by order_date, order_id, line_number
    ) as previous_purchase_date,
    lead(order_date) over (
        partition by customer_id order by order_date, order_id, line_number
    ) as next_purchase_date
from {{ ref('int_sales_enriched') }}
