CREATE SCHEMA IF NOT EXISTS dataflow_control;

CREATE TABLE IF NOT EXISTS dataflow_control.adf_watermarks (
    pipeline_name text PRIMARY KEY,
    watermark_value timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE dataflow_control.adf_watermarks IS
    'Watermarks for Azure Data Factory incremental extraction; update only after a successful ADLS copy.';
