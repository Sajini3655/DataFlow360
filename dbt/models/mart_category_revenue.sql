select
    category,
    count(*) as orders,
    sum(quantity) as units,
    sum(revenue) as revenue
from {{ source('dataflow360', 'clean_sales') }}
group by category
