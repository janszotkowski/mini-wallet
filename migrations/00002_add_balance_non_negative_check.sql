-- +goose Up
ALTER TABLE accounts
    ADD CONSTRAINT accounts_balance_non_negative CHECK (balance >= 0);

-- +goose Down
ALTER TABLE accounts
    DROP CONSTRAINT accounts_balance_non_negative;