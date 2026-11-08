-- +goose Up
CREATE TABLE outbox_events (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

-- +goose Down
DROP TABLE outbox_events;