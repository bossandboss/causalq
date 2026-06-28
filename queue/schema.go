package queue

const SQLiteSchema = `
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    content_hash TEXT UNIQUE,
    payload TEXT NOT NULL,
    context TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    priority_score REAL DEFAULT 0.0,
    base_priority INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    retries_count INTEGER DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reserved_at TIMESTAMP,
    completed_at TIMESTAMP,
    predicted_time REAL DEFAULT 1.0,
    real_cpu_time REAL DEFAULT 0.0,
    is_anomaly INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_jobs_state_score ON jobs(state, priority_score DESC, run_at ASC);
CREATE INDEX IF NOT EXISTS idx_jobs_content_hash ON jobs(content_hash);

CREATE TABLE IF NOT EXISTS job_metrics (
    context TEXT PRIMARY KEY,
    avg_duration REAL DEFAULT 1.0,
    execution_count INTEGER DEFAULT 0
);
`
