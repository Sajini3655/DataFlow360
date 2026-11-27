# SQL analytics examples

These queries target the current PostgreSQL warehouse and preserve the sales
line grain `(order_id, line_number)`.

## Monthly revenue and growth

```sql
with monthly as (
    select date_trunc('month', order_date)::date as month,
           region,
           sum(revenue) as revenue
    from silver_sales
    group by 1, 2
)
select month, region, revenue,
       sum(revenue) over (
           partition by region order by month
           rows between unbounded preceding and current row
       ) as running_revenue,
       lag(revenue) over (partition by region order by month) as prior_month,
       revenue - lag(revenue) over (
           partition by region order by month
       ) as revenue_change
from monthly
order by region, month;
```

## Rank products within categories

```sql
with product_sales as (
    select product, category,
           sum(quantity) as units,
           sum(revenue) as revenue
    from silver_sales
    group by product, category
)
select *,
       rank() over (partition by category order by revenue desc) as revenue_rank,
       dense_rank() over (order by revenue desc) as overall_rank
from product_sales
order by category, revenue_rank;
```

## Conditional aggregation and inventory

```sql
select p.product_name,
       sum(i.quantity_sold) filter (where d.month between 1 and 3) as q1_units,
       sum(i.quantity_sold) filter (where d.month between 4 and 6) as q2_units,
       avg(i.closing_stock) as average_closing_stock,
       sum(i.quantity_sold)::numeric / nullif(sum(i.opening_stock), 0) as turnover
from inventory_fact i
join dim_product p on p.product_key = i.product_key
join dim_date d on d.date_key = i.date_key
group by p.product_name
order by turnover desc nulls last;
```

## Query-plan inspection

Use representative filters and inspect actual plans before adding indexes:

```sql
explain (analyze, buffers)
select region, category, sum(revenue)
from silver_sales
where order_date >= date '2026-01-01'
  and order_date < date '2026-04-01'
group by region, category;
```

`EXPLAIN ANALYZE` executes the query. Use read-only analytical statements and
avoid running it on mutation statements against shared data.
