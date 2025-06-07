CREATE TABLE transaction_projections (
                                         id UUID PRIMARY KEY,
                                         account_id UUID NOT NULL REFERENCES account_projections(id),
                                         transaction_type VARCHAR(50) NOT NULL,
                                         amount DECIMAL(19,4) NOT NULL,
                                         timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_transaction_projections_account ON transaction_projections(account_id);
CREATE INDEX idx_transaction_projections_timestamp ON transaction_projections(timestamp);
CREATE INDEX idx_transaction_projections_type ON transaction_projections(transaction_type);