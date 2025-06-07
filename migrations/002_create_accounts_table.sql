-- 3. Optimized projection tables
CREATE TABLE IF NOT EXISTS account_projections (
                                                   id UUID PRIMARY KEY,
                                                   owner_name VARCHAR(255) NOT NULL,
                                                   balance DECIMAL(20,2) NOT NULL DEFAULT 0.00,
                                                   is_active BOOLEAN NOT NULL DEFAULT true,
                                                   created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                                                   updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for account projections
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_account_projections_owner_name
    ON account_projections (owner_name);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_account_projections_is_active
    ON account_projections (is_active);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_account_projections_created_at
    ON account_projections (created_at);
