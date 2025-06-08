CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
-- High-performance database schema optimizations

CREATE INDEX idx_snapshots_created_at ON snapshots(created_at);

-- 1. Events table with optimized partitioning and indexes
CREATE TABLE IF NOT EXISTS events (
    id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    event_data JSONB NOT NULL,
    version BIGINT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
) PARTITION BY RANGE (timestamp);

-- Create monthly partitions with optimized fillfactor
CREATE TABLE events_2024_01 PARTITION OF events
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')
    WITH (fillfactor = 90);
CREATE TABLE events_2024_02 PARTITION OF events
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01')
    WITH (fillfactor = 90);
-- ... continue for other months

-- Optimized indexes for events with fillfactor
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_aggregate_version
    ON events (aggregate_id, version)
    WITH (fillfactor = 90);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_timestamp
    ON events (timestamp)
    WITH (fillfactor = 90);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_event_type
    ON events (event_type)
    WITH (fillfactor = 90);

-- Add partial index for active events
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_recent
    ON events (timestamp)
    WHERE timestamp > NOW() - INTERVAL '30 days'
    WITH (fillfactor = 90);

-- Unique constraint for event ordering with fillfactor
ALTER TABLE events ADD CONSTRAINT unique_aggregate_version
    UNIQUE (aggregate_id, version)
    WITH (fillfactor = 90);

-- 2. Snapshots table with optimized indexes
CREATE TABLE IF NOT EXISTS snapshots (
    aggregate_id UUID PRIMARY KEY,
    snapshot_data JSONB NOT NULL,
    version BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
) WITH (fillfactor = 90);

-- Optimized indexes for snapshots
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_snapshots_created_at
    ON snapshots (created_at)
    WITH (fillfactor = 90);

-- Add index for version lookups
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_snapshots_version
    ON snapshots (version)
    WITH (fillfactor = 90);

-- Add GIN index for JSONB queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_snapshots_data_gin
    ON snapshots USING GIN (snapshot_data jsonb_path_ops)
    WITH (fillfactor = 90);
