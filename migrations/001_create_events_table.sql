CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE events (
                        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                        aggregate_id UUID NOT NULL,
                        event_type VARCHAR(255) NOT NULL,
                        event_data JSONB NOT NULL,
                        version BIGINT NOT NULL,
                        timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        UNIQUE(aggregate_id, version)
);

CREATE INDEX idx_events_aggregate_id ON events(aggregate_id);
CREATE INDEX idx_events_timestamp ON events(timestamp);
CREATE INDEX idx_events_type ON events(event_type);