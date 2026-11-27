# Data dictionary

Primary data types below are the PostgreSQL view types. The warehouse provides
no cost, profit, discount, or reorder-point columns.

## Sales and inventory

| Table | Columns |
|---|---|
| Fact Sales | `order_id` text; `line_number` integer; `sales_line_id` text; `customer_key`, `product_key`, `date_key`, `region_key`, `supplier_key` integers; `customer_id`, `customer_name`, `product_name`, `category_name`, `supplier_id`, `supplier_name`, `region_name` text; `full_date` date; `year`, `quarter`, `month`, `quantity` integers; `month_name` text; `unit_price`, `revenue` numeric; `updated_at` timestamp with time zone |
| Fact Inventory | `inventory_key`, dimension keys integers; `product_name`, `category_name`, `supplier_id`, `supplier_name`, `region_name` text; `full_date` date; `year`, `quarter`, `month`, `opening_stock`, `quantity_sold`, `closing_stock` integers |

## Dimensions

| Table | Columns |
|---|---|
| Dim Customer | `customer_key` integer, `customer_id`, `customer_name`, `region` text |
| Dim Product | `product_key`, `category_key` integers, `product_name`, `category_name` text |
| Dim Date | `date_key`, `year`, `quarter`, `month` integers, `full_date`, `month_start` dates, `month_name` text |
| Dim Region | `region_key` integer, `region_name` text |
| Dim Supplier | `supplier_key` integer, `supplier_id`, `supplier_name`, `region` text |

## Data engineering facts

| Table | Measures / attributes |
|---|---|
| Fact Pipeline Execution | `pipeline_run_id`, `source_name`, `state`, `engine`, timestamps and `started_date`, nullable `duration_seconds`, `rows_read`, `accepted_rows`, `rejected_rows` |
| Fact Processing Execution | `execution_id`, optional `benchmark_id` and `pipeline_run_id`, `engine`, `dataset_name`, input/accepted/rejected rows, duration, throughput, output bytes, state, `created_at`, `created_date` |
| Fact Data Quality | `result_id`, `pipeline_run_id`, source/dataset/layer, rule ID/name, dimension, severity, state, checked/failure counts and rates, threshold, `checked_at`, `checked_date` |
| Fact Quality Run | `pipeline_run_id`, source, checked/accepted/rejected rows, acceptance/rejection rates, overall score, `checked_at`, `checked_date` |

Optional dbt sources include `analytics.fct_sales_line`,
`analytics.monthly_sales`, `analytics.customer_lifetime_value`,
`analytics.regional_revenue`, and other existing analytics marts. Their
availability depends on dbt having built the models in the selected database.
