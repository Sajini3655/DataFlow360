with customer_totals as (
    select
        customer_id,
        max(customer_name) as customer_name,
        min(order_date) as first_purchase_date,
        max(order_date) as last_purchase_date,
        count(distinct order_id) as order_count,
        sum(revenue) as lifetime_revenue,
        avg(revenue) as average_line_revenue
    from {{ ref('fct_sales_line') }}
    group by customer_id
)
select
    customer.*,
    (
        select threshold.segment_name
        from {{ ref('customer_segment_thresholds') }} threshold
        where customer.lifetime_revenue >= threshold.minimum_revenue
        order by threshold.minimum_revenue desc
        limit 1
    ) as customer_segment
from customer_totals customer
