CREATE TABLE IF NOT EXISTS sales(
    order_id text PRIMARY KEY,
    product text NOT NULL,
    category text NOT NULL,
    quantity integer NOT NULL CHECK(quantity>0),
    unit_price numeric(12,2) NOT NULL CHECK(unit_price>=0)
);

CREATE TABLE IF NOT EXISTS pipeline_runs(
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    accepted integer NOT NULL,
    rejected integer NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw_sales(
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_name text NOT NULL,
    source_row_number integer NOT NULL,
    payload jsonb NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rejected_sales(
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_run_id bigint REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    source_name text NOT NULL,
    source_row_number integer,
    record jsonb NOT NULL,
    reason text NOT NULL,
    rejected_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dim_category(
    category_key integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    category_name text NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_product(
    product_key integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_name text NOT NULL,
    category_key integer NOT NULL REFERENCES dim_category(category_key),
    UNIQUE(product_name, category_key)
);

CREATE TABLE IF NOT EXISTS fact_sales(
    order_id text PRIMARY KEY,
    product_key integer NOT NULL REFERENCES dim_product(product_key),
    category_key integer NOT NULL REFERENCES dim_category(category_key),
    quantity integer NOT NULL CHECK(quantity>0),
    unit_price numeric(12,2) NOT NULL CHECK(unit_price>=0),
    revenue numeric(14,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ingestion_watermarks(
    source_name text PRIMARY KEY,
    source_hash text NOT NULL,
    last_processed_at timestamptz NOT NULL DEFAULT now(),
    rows_read integer NOT NULL DEFAULT 0,
    rows_accepted integer NOT NULL DEFAULT 0,
    rows_rejected integer NOT NULL DEFAULT 0
);

CREATE OR REPLACE VIEW clean_sales AS
SELECT
    order_id,
    product,
    category,
    quantity,
    unit_price,
    quantity * unit_price AS revenue
FROM sales;

CREATE TABLE IF NOT EXISTS bronze_sales(
    source_row_number bigint,
    source_name text NOT NULL,
    payload jsonb NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(source_name, source_row_number)
);

CREATE TABLE IF NOT EXISTS silver_sales(
    order_id text NOT NULL,
    line_number integer NOT NULL DEFAULT 1,
    customer_id text,
    customer_name text,
    product_id text,
    product text NOT NULL,
    category text NOT NULL,
    supplier_id text,
    region text,
    order_date date,
    quantity integer NOT NULL CHECK(quantity>0),
    unit_price numeric(12,2) NOT NULL CHECK(unit_price>=0),
    revenue numeric(14,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    transformed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(order_id,line_number)
);

CREATE TABLE IF NOT EXISTS dim_customer(
    customer_key integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id text NOT NULL UNIQUE,
    customer_name text NOT NULL,
    region text NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_date(
    date_key integer PRIMARY KEY,
    full_date date NOT NULL UNIQUE,
    year integer NOT NULL,
    month integer NOT NULL,
    month_name text NOT NULL,
    quarter integer NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_region(
    region_key integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    region_name text NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_supplier(
    supplier_key integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    supplier_id text NOT NULL UNIQUE,
    supplier_name text NOT NULL,
    region text NOT NULL
);

CREATE TABLE IF NOT EXISTS gold_sales_fact(
    order_id text NOT NULL,
    line_number integer NOT NULL DEFAULT 1,
    customer_key integer NOT NULL REFERENCES dim_customer(customer_key),
    product_key integer NOT NULL REFERENCES dim_product(product_key),
    date_key integer NOT NULL REFERENCES dim_date(date_key),
    region_key integer NOT NULL REFERENCES dim_region(region_key),
    supplier_key integer NOT NULL REFERENCES dim_supplier(supplier_key),
    quantity integer NOT NULL CHECK(quantity>0),
    unit_price numeric(12,2) NOT NULL CHECK(unit_price>=0),
    revenue numeric(14,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(order_id,line_number)
);

CREATE TABLE IF NOT EXISTS inventory_fact(
    inventory_key integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_key integer NOT NULL REFERENCES dim_product(product_key),
    supplier_key integer NOT NULL REFERENCES dim_supplier(supplier_key),
    region_key integer NOT NULL REFERENCES dim_region(region_key),
    date_key integer NOT NULL REFERENCES dim_date(date_key),
    opening_stock integer NOT NULL,
    quantity_sold integer NOT NULL DEFAULT 0,
    closing_stock integer NOT NULL,
    UNIQUE(product_key,date_key)
);

CREATE TABLE IF NOT EXISTS source_adapters(
    adapter_name text PRIMARY KEY,
    status text NOT NULL,
    verified_at timestamptz NOT NULL DEFAULT now(),
    last_record_count bigint NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS pipeline_jobs(
    job_id text PRIMARY KEY,
    dag_run_id text UNIQUE NOT NULL,
    source_name text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
