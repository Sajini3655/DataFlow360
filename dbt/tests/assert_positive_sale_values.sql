select *
from {{ ref('fct_sales_line') }}
where quantity <= 0
   or unit_price < 0
   or revenue < 0
