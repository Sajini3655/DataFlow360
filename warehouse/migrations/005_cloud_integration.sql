CREATE TABLE IF NOT EXISTS cloud_operation_events (
    event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    operation text NOT NULL,
    storage_mode text NOT NULL,
    state text NOT NULL,
    layer text,
    object_name text,
    bytes_transferred bigint NOT NULL DEFAULT 0,
    message text,
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cloud_operation_events_created
    ON cloud_operation_events (started_at DESC, event_id DESC);
CREATE INDEX IF NOT EXISTS idx_cloud_operation_events_operation
    ON cloud_operation_events (operation, state, started_at DESC);
