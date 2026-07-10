CREATE TABLE IF NOT EXISTS pipeline_events (
    id BIGSERIAL PRIMARY KEY,
    pipeline_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    old_state TEXT,
    new_state TEXT,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS backpressure_events (
    id BIGSERIAL PRIMARY KEY,
    pipeline_id TEXT NOT NULL,
    action TEXT NOT NULL,
    sanctuary_len INT NOT NULL,
    sanctuary_cap INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS disruption_sessions (
    id BIGSERIAL PRIMARY KEY,
    pipeline_id TEXT NOT NULL,
    source_addr TEXT,
    target_addr TEXT,
    held_at TIMESTAMPTZ NOT NULL,
    drained_at TIMESTAMPTZ,
    duration_us BIGINT,
    backpressure_triggered BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_pipeline_events_pipeline_id ON pipeline_events(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_events_created_at ON pipeline_events(created_at);
CREATE INDEX IF NOT EXISTS idx_backpressure_events_created_at ON backpressure_events(created_at);
CREATE INDEX IF NOT EXISTS idx_disruption_sessions_pipeline_id ON disruption_sessions(pipeline_id);
