CREATE TABLE account_projections (
                                     id UUID PRIMARY KEY,
                                     owner_name VARCHAR(255) NOT NULL,
                                     balance DECIMAL(19,4) NOT NULL DEFAULT 0,
                                     is_active BOOLEAN NOT NULL DEFAULT true,
                                     created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                                     updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_account_projections_owner ON account_projections(owner_name);
CREATE INDEX idx_account_projections_active ON account_projections(is_active);