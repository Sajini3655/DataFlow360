with monthly as (
    select
        date_trunc('month', order_date)::date as sales_month,
        region_name,
        sum(revenue) as revenue,
        sum(quantity) as units_sold
    from {{ ref('fct_sales_line') }}
    group by date_trunc('month', order_date), region_name
)
select
    sales_month,
    region_name,
    revenue,
    units_sold,
    sum(revenue) over (
        partition by region_name order by sales_month
        rows between unbounded preceding and current row
    ) as running_revenue,
    avg(revenue) over (
        partition by region_name order by sales_month
        rows between 2 preceding and current row
    ) as rolling_three_month_revenue,
    lag(revenue) over (
        partition by region_name order by sales_month
    ) as previous_month_revenue,
    case
        when lag(revenue) over (partition by region_name order by sales_month) is null
            or lag(revenue) over (partition by region_name order by sales_month) = 0
        then null
        else revenue / lag(revenue) over (
            partition by region_name order by sales_month
        ) - 1
    end as revenue_growth_rate
from monthly
