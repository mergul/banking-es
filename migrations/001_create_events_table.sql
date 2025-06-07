CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
-- High-performance database schema optimizations

CREATE INDEX idx_snapshots_created_at ON snapshots(created_at);

-- 1. Events table with partitioning and optimized indexes
CREATE TABLE IF NOT EXISTS events (
                                      id UUID PRIMARY KEY,
                                      aggregate_id UUID NOT NULL,
                                      event_type VARCHAR(100) NOT NULL,
                                      event_data JSONB NOT NULL,
                                      version BIGINT NOT NULL,
                                      timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
) PARTITION BY RANGE (timestamp);

-- Create monthly partitions (automate this in production)
CREATE TABLE events_2024_01 PARTITION OF events
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE events_2024_02 PARTITION OF events
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
-- ... continue for other months

-- Optimized indexes for events
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_aggregate_version
    ON events (aggregate_id, version);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_timestamp
    ON events (timestamp);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_event_type
    ON events (event_type);

-- Unique constraint for event ordering
ALTER TABLE events ADD CONSTRAINT unique_aggregate_version
    UNIQUE (aggregate_id, version);

-- 2. Snapshots table for aggregate state caching
CREATE TABLE IF NOT EXISTS snapshots (
                                         aggregate_id UUID PRIMARY KEY,
                                         snapshot_data JSONB NOT NULL,
                                         version BIGINT NOT NULL,
                                         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_snapshots_created_at
    ON snapshots (created_at);
