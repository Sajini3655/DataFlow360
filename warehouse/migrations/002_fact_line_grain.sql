ALTER TABLE silver_sales
    ADD COLUMN IF NOT EXISTS line_number integer NOT NULL DEFAULT 1;

DO $$
DECLARE constraint_name text;
BEGIN
    SELECT conname INTO constraint_name
    FROM pg_constraint
    WHERE conrelid = 'silver_sales'::regclass AND contype = 'p';
    IF constraint_name IS NOT NULL AND NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'silver_sales'::regclass
          AND contype = 'p'
          AND pg_get_constraintdef(oid) = 'PRIMARY KEY (order_id, line_number)'
    ) THEN
        EXECUTE format('ALTER TABLE silver_sales DROP CONSTRAINT %I', constraint_name);
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'silver_sales'::regclass
          AND contype = 'p'
          AND pg_get_constraintdef(oid) = 'PRIMARY KEY (order_id, line_number)'
    ) THEN
        ALTER TABLE silver_sales ADD PRIMARY KEY(order_id,line_number);
    END IF;
END $$;

ALTER TABLE gold_sales_fact
    ADD COLUMN IF NOT EXISTS line_number integer NOT NULL DEFAULT 1;

DO $$
DECLARE constraint_name text;
BEGIN
    SELECT conname INTO constraint_name
    FROM pg_constraint
    WHERE conrelid = 'gold_sales_fact'::regclass AND contype = 'p';
    IF constraint_name IS NOT NULL AND NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'gold_sales_fact'::regclass
          AND contype = 'p'
          AND pg_get_constraintdef(oid) = 'PRIMARY KEY (order_id, line_number)'
    ) THEN
        EXECUTE format('ALTER TABLE gold_sales_fact DROP CONSTRAINT %I', constraint_name);
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'gold_sales_fact'::regclass
          AND contype = 'p'
          AND pg_get_constraintdef(oid) = 'PRIMARY KEY (order_id, line_number)'
    ) THEN
        ALTER TABLE gold_sales_fact ADD PRIMARY KEY(order_id,line_number);
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS pipeline_jobs(
    job_id text PRIMARY KEY,
    dag_run_id text UNIQUE NOT NULL,
    source_name text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
