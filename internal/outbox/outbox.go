package outbox

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

type pendingEvent struct {
	ID        string
	EventType string
	Payload   []byte
}

func PublishPending(ctx context.Context, pool *pgxpool.Pool, client *kgo.Client) (int, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx,
		`SELECT id::text, event_type, payload
		 FROM outbox_events
		 WHERE published_at IS NULL
		 ORDER BY id
		 LIMIT 100
		 FOR UPDATE SKIP LOCKED`,
	)

	if err != nil {
		return 0, fmt.Errorf("select pending events: %w", err)
	}
	defer rows.Close()

	var events []pendingEvent
	for rows.Next() {
		var event pendingEvent
		if err := rows.Scan(&event.ID, &event.EventType, &event.Payload); err != nil {
			return 0, fmt.Errorf("scan event: %w", err)
		}
		events = append(events, event) // append vrací nový slice, proto se výsledek musí přiřadit zpátky.
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("read events: %w", err)
	}

	for _, event := range events {
		record := &kgo.Record{
			Topic: event.EventType,
			Key:   []byte(event.ID),
			Value: event.Payload,
		}
		if err := client.ProduceSync(ctx, record).FirstErr(); err != nil { // Pošle zprávu a počká, až ji Kafka potvrdí, jako await producer.send(...).
			return 0, fmt.Errorf("produce event %s: %w", event.ID, err)
		}

		_, err = tx.Exec(ctx, `UPDATE outbox_events SET published_at = now() WHERE id = $1`, event.ID)
		if err != nil {
			return 0, fmt.Errorf("mark event %s as published: %w", event.ID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit transaction: %w", err)
	}

	return len(events), nil
}
