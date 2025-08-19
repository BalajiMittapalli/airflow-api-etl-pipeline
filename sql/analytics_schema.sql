-- Analytics table schema for GitHub Events
-- This table is created automatically by the postgres_loader.py

CREATE TABLE IF NOT EXISTS github_events (
    event_id INTEGER PRIMARY KEY,
    event_type VARCHAR(500),
    event_time TIMESTAMP,
    repo_id INTEGER,
    user_login VARCHAR(500),
    ingestion_timestamp TIMESTAMP,
    source VARCHAR(500)
);

-- Pipeline monitoring table
-- This table tracks all ETL pipeline runs

CREATE TABLE IF NOT EXISTS pipeline_monitor (
    run_id UUID PRIMARY KEY,
    dag_id VARCHAR(255) NOT NULL,
    run_date VARCHAR(10) NOT NULL,  -- YYYY-MM-DD format
    rows_processed INTEGER NOT NULL,
    duration_sec FLOAT NOT NULL,
    status VARCHAR(20) NOT NULL,  -- 'success' or 'failed'
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Example queries for monitoring

-- Check recent pipeline runs
SELECT 
    dag_id,
    run_date,
    rows_processed,
    duration_sec,
    status,
    created_at
FROM pipeline_monitor 
ORDER BY created_at DESC 
LIMIT 10;

-- Check failed runs
SELECT 
    dag_id,
    run_date,
    error_message,
    created_at
FROM pipeline_monitor 
WHERE status = 'failed'
ORDER BY created_at DESC;

-- Count events by type
SELECT 
    event_type,
    COUNT(*) as event_count
FROM github_events 
GROUP BY event_type 
ORDER BY event_count DESC;

-- Check data freshness
SELECT 
    MAX(ingestion_timestamp) as latest_ingestion,
    COUNT(*) as total_events
FROM github_events;
