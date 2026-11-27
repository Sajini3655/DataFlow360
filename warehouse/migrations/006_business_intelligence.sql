CREATE SCHEMA IF NOT EXISTS bi;

CREATE OR REPLACE VIEW bi.dim_customer AS
SELECT customer_key, customer_id, customer_name, region
FROM public.dim_customer;

CREATE OR REPLACE VIEW bi.dim_product AS
SELECT p.product_key, p.product_name, p.category_key, c.category_name
FROM public.dim_product p
JOIN public.dim_category c ON c.category_key = p.category_key;

CREATE OR REPLACE VIEW bi.dim_date AS
SELECT date_key, full_date, year, quarter, month, month_name,
       date_trunc('month', full_date)::date AS month_start
FROM public.dim_date;

CREATE OR REPLACE VIEW bi.dim_region AS
SELECT region_key, region_name
FROM public.dim_region;

CREATE OR REPLACE VIEW bi.dim_supplier AS
SELECT supplier_key, supplier_id, supplier_name, region
FROM public.dim_supplier;

CREATE OR REPLACE VIEW bi.fact_sales AS
SELECT
    f.order_id,
    f.line_number,
    f.order_id || ':' || f.line_number::text AS sales_line_id,
    f.customer_key,
    c.customer_id,
    c.customer_name,
    f.product_key,
    p.product_name,
    p.category_key,
    cat.category_name,
    f.date_key,
    d.full_date,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    f.region_key,
    r.region_name,
    f.supplier_key,
    s.supplier_id,
    s.supplier_name,
    f.quantity,
    f.unit_price,
    f.revenue,
    f.updated_at
FROM public.gold_sales_fact f
JOIN public.dim_customer c ON c.customer_key = f.customer_key
JOIN public.dim_product p ON p.product_key = f.product_key
JOIN public.dim_category cat ON cat.category_key = p.category_key
JOIN public.dim_date d ON d.date_key = f.date_key
JOIN public.dim_region r ON r.region_key = f.region_key
JOIN public.dim_supplier s ON s.supplier_key = f.supplier_key;

CREATE OR REPLACE VIEW bi.fact_inventory AS
SELECT
    i.inventory_key,
    i.product_key,
    p.product_name,
    p.category_key,
    p.category_name,
    i.supplier_key,
    s.supplier_id,
    s.supplier_name,
    i.region_key,
    r.region_name,
    i.date_key,
    d.full_date,
    d.year,
    d.quarter,
    d.month,
    i.opening_stock,
    i.quantity_sold,
    i.closing_stock
FROM public.inventory_fact i
JOIN bi.dim_product p ON p.product_key = i.product_key
JOIN bi.dim_supplier s ON s.supplier_key = i.supplier_key
JOIN bi.dim_region r ON r.region_key = i.region_key
JOIN bi.dim_date d ON d.date_key = i.date_key;

CREATE OR REPLACE VIEW bi.fact_pipeline_execution AS
SELECT
    id AS pipeline_run_id,
    source_name,
    state,
    engine,
    started_at,
    started_at::date AS started_date,
    finished_at,
    CASE
        WHEN finished_at IS NOT NULL AND started_at IS NOT NULL
        THEN extract(epoch FROM finished_at - started_at)::numeric(14,4)
        ELSE NULL
    END AS duration_seconds,
    rows_read,
    accepted AS accepted_rows,
    rejected AS rejected_rows
FROM public.pipeline_runs;

CREATE OR REPLACE VIEW bi.fact_processing_execution AS
SELECT
    execution_id,
    benchmark_id,
    pipeline_run_id,
    engine,
    dataset_name,
    input_rows,
    accepted_rows,
    rejected_rows,
    duration_seconds,
    throughput_rows_per_second,
    output_bytes,
    state,
    created_at,
    created_at::date AS created_date
FROM public.processing_executions;

CREATE OR REPLACE VIEW bi.fact_data_quality AS
SELECT
    q.result_id,
    q.pipeline_run_id,
    q.source_name,
    q.dataset_name,
    q.data_layer,
    q.rule_id,
    r.rule_name,
    r.dimension,
    r.severity,
    q.state,
    q.checked_count,
    q.failure_count,
    q.failure_rate,
    q.threshold_percent,
    q.checked_at,
    q.checked_at::date AS checked_date
FROM public.data_quality_results q
JOIN public.data_quality_rules r ON r.rule_id = q.rule_id;

CREATE OR REPLACE VIEW bi.fact_quality_run AS
SELECT
    q.pipeline_run_id,
    q.source_name,
    q.rows_checked,
    q.rows_accepted,
    q.rows_rejected,
    q.acceptance_rate,
    q.rejection_rate,
    q.overall_score,
    q.checked_at,
    q.checked_at::date AS checked_date
FROM public.quality_run_summaries q;
