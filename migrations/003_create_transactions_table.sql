-- 4. Transaction projections with partitioning
CREATE TABLE IF NOT EXISTS transaction_projections (
                                                       id UUID PRIMARY KEY,
                                                       account_id UUID NOT NULL,
                                                       transaction_type VARCHAR(50) NOT NULL,
                                                       amount DECIMAL(20,2) NOT NULL,
                                                       timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
) PARTITION BY RANGE (timestamp);

-- Create monthly partitions for transactions
CREATE TABLE transaction_projections_2024_01 PARTITION OF transaction_projections
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE transaction_projections_2024_02 PARTITION OF transaction_projections
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
-- ... continue for other months

-- Optimized indexes for transaction projections
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transaction_projections_account_id
    ON transaction_projections (account_id, timestamp DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transaction_projections_type
    ON transaction_projections (transaction_type);
