ALTER TABLE pipeline_runs
    ADD COLUMN IF NOT EXISTS source_name text NOT NULL DEFAULT 'legacy',
    ADD COLUMN IF NOT EXISTS state text NOT NULL DEFAULT 'succeeded',
    ADD COLUMN IF NOT EXISTS airflow_run_id text,
    ADD COLUMN IF NOT EXISTS engine text NOT NULL DEFAULT 'pandas',
    ADD COLUMN IF NOT EXISTS started_at timestamptz,
    ADD COLUMN IF NOT EXISTS finished_at timestamptz,
    ADD COLUMN IF NOT EXISTS error_message text,
    ADD COLUMN IF NOT EXISTS rows_read integer NOT NULL DEFAULT 0;

ALTER TABLE pipeline_runs ALTER COLUMN state SET DEFAULT 'running';
ALTER TABLE rejected_sales
    ADD COLUMN IF NOT EXISTS corrected_record jsonb,
    ADD COLUMN IF NOT EXISTS reprocessed_at timestamptz;
ALTER TABLE pipeline_jobs
    ADD COLUMN IF NOT EXISTS engine text NOT NULL DEFAULT 'pandas';

CREATE TABLE IF NOT EXISTS pipeline_stage_runs (
    pipeline_run_id bigint NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    stage_name text NOT NULL,
    attempt integer NOT NULL DEFAULT 1,
    state text NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    rows_read integer NOT NULL DEFAULT 0,
    rows_written integer NOT NULL DEFAULT 0,
    error_message text,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (pipeline_run_id, stage_name, attempt)
);

CREATE TABLE IF NOT EXISTS data_quality_rules (
    rule_id text PRIMARY KEY,
    rule_name text NOT NULL,
    description text NOT NULL,
    dimension text NOT NULL,
    severity text NOT NULL CHECK (severity IN ('info', 'warning', 'error', 'critical')),
    threshold_percent numeric(5,2) NOT NULL DEFAULT 0
        CHECK (threshold_percent >= 0 AND threshold_percent <= 100),
    enabled boolean NOT NULL DEFAULT true,
    config jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS quality_run_summaries (
    pipeline_run_id bigint PRIMARY KEY REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    source_name text NOT NULL,
    rows_checked integer NOT NULL DEFAULT 0,
    rows_accepted integer NOT NULL DEFAULT 0,
    rows_rejected integer NOT NULL DEFAULT 0,
    acceptance_rate numeric(7,4) NOT NULL DEFAULT 0,
    rejection_rate numeric(7,4) NOT NULL DEFAULT 0,
    overall_score numeric(7,4) NOT NULL DEFAULT 0,
    checked_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS data_quality_results (
    result_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_run_id bigint NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    source_name text NOT NULL,
    dataset_name text NOT NULL,
    data_layer text NOT NULL,
    rule_id text NOT NULL REFERENCES data_quality_rules(rule_id),
    state text NOT NULL CHECK (state IN ('passed', 'failed', 'skipped')),
    checked_count integer NOT NULL DEFAULT 0,
    failure_count integer NOT NULL DEFAULT 0,
    failure_rate numeric(7,4) NOT NULL DEFAULT 0,
    threshold_percent numeric(5,2) NOT NULL DEFAULT 0,
    checked_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (pipeline_run_id, rule_id)
);

CREATE INDEX IF NOT EXISTS ix_quality_results_source_checked
    ON data_quality_results (source_name, checked_at DESC);
CREATE INDEX IF NOT EXISTS ix_quality_results_rule_checked
    ON data_quality_results (rule_id, checked_at DESC);

CREATE TABLE IF NOT EXISTS source_schema_snapshots (
    source_name text PRIMARY KEY,
    observed_schema jsonb NOT NULL,
    observed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS schema_drift_events (
    drift_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_run_id bigint REFERENCES pipeline_runs(id) ON DELETE SET NULL,
    source_name text NOT NULL,
    previous_schema jsonb NOT NULL,
    observed_schema jsonb NOT NULL,
    changes jsonb NOT NULL,
    detected_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_schema_drift_source_detected
    ON schema_drift_events (source_name, detected_at DESC);

CREATE TABLE IF NOT EXISTS lineage_events (
    lineage_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_run_id bigint REFERENCES pipeline_runs(id) ON DELETE SET NULL,
    source_name text NOT NULL,
    from_dataset text NOT NULL,
    to_dataset text NOT NULL,
    from_layer text NOT NULL,
    to_layer text NOT NULL,
    rows_written integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS ix_lineage_run_created
    ON lineage_events (pipeline_run_id, created_at);

CREATE TABLE IF NOT EXISTS processing_executions (
    execution_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    benchmark_id text,
    pipeline_run_id bigint REFERENCES pipeline_runs(id) ON DELETE SET NULL,
    engine text NOT NULL CHECK (engine IN ('pandas', 'pyspark')),
    dataset_name text NOT NULL,
    input_rows integer NOT NULL DEFAULT 0,
    accepted_rows integer NOT NULL DEFAULT 0,
    rejected_rows integer NOT NULL DEFAULT 0,
    duration_seconds numeric(14,4) NOT NULL DEFAULT 0,
    throughput_rows_per_second numeric(16,4) NOT NULL DEFAULT 0,
    output_bytes bigint NOT NULL DEFAULT 0,
    state text NOT NULL CHECK (state IN ('succeeded', 'failed')),
    resource_metrics jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO data_quality_rules (
    rule_id, rule_name, description, dimension, severity, threshold_percent, config
) VALUES
    ('required_fields', 'Required field completeness',
     'Each row contains the required sales line fields.', 'completeness', 'error', 2,
     '{"columns":["order_id","product","category","quantity","unit_price","order_date"]}'),
    ('unique_business_key', 'Unique sales line key',
     'The (order_id, line_number) grain is unique within each source dataset.', 'uniqueness', 'error', 2, '{}'),
    ('valid_quantity', 'Positive integer quantity',
     'Quantity is an integer greater than zero.', 'validity', 'error', 2, '{}'),
    ('valid_unit_price', 'Non-negative finite unit price',
     'Unit price is a finite numeric value greater than or equal to zero.', 'validity', 'error', 2, '{}'),
    ('valid_order_date', 'Valid order date',
     'Order date uses a valid ISO date representation.', 'timeliness', 'error', 2, '{}'),
    ('referential_integrity', 'Warehouse dimension integrity',
     'Accepted sales lines resolve to all required warehouse dimensions.', 'referential_integrity', 'critical', 0, '{}'),
    ('revenue_consistency', 'Revenue reconciliation',
     'Warehouse line revenue reconciles to quantity multiplied by unit price.', 'accuracy', 'critical', 0, '{}'),
    ('quantity_range', 'Quantity range',
     'Quantity remains within the configured operational range.', 'validity', 'warning', 0,
     '{"column":"quantity","min":1,"max":1000000}'),
    ('accepted_values', 'Accepted product categories',
     'Category values belong to the configured set.', 'consistency', 'warning', 0,
     '{"column":"category","values":["Electronics","Furniture","Office Supplies"]}'),
    ('valid_discount', 'Discount range',
     'Optional discounts are between zero and one inclusive.', 'validity', 'warning', 0,
     '{"column":"discount","min":0,"max":1}'),
    ('date_range', 'Order date range',
     'Order dates remain within the configured reporting window.', 'timeliness', 'warning', 0,
     '{"column":"order_date","start_date":"2026-01-01","end_date":"2026-12-31"}')
ON CONFLICT (rule_id) DO NOTHING;
