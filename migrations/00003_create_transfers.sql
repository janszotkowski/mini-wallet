-- +goose Up
CREATE TABLE transfers (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    idempotency_key TEXT NOT NULL UNIQUE,
    from_account_id UUID NOT NULL REFERENCES accounts (id),
    to_account_id UUID NOT NULL REFERENCES accounts (id),
    amount BIGINT NOT NULL CHECK (amount > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE transfers;