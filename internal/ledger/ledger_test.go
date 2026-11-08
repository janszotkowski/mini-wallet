package ledger

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Fatal("DATABASE_URL není nastavená")
	}

	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	return pool
}

func createAccount(t *testing.T, pool *pgxpool.Pool, balance int64) string {
	t.Helper()

	var id string
	err := pool.QueryRow(context.Background(),
		`INSERT INTO accounts (name, balance) VALUES ('test', $1) RETURNING id::text`,
		balance,
	).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}

	return id
}

func getBalance(t *testing.T, pool *pgxpool.Pool, id string) int64 {
	t.Helper()

	var balance int64
	err := pool.QueryRow(context.Background(),
		`SELECT balance FROM accounts WHERE id = $1`, id,
	).Scan(&balance)
	if err != nil {
		t.Fatal(err)
	}

	return balance
}

func TestTransfer(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	from := createAccount(t, pool, 1000)
	to := createAccount(t, pool, 0)

	if _, err := Transfer(ctx, pool, uuid.NewString(), from, to, 300); err != nil {
		t.Fatal(err)
	}

	if got := getBalance(t, pool, from); got != 700 {
		t.Errorf("zůstatek from = %d, čekám 700", got)
	}
	if got := getBalance(t, pool, to); got != 300 {
		t.Errorf("zůstatek to = %d, čekám 300", got)
	}

	var entries, sum int64
	err := pool.QueryRow(ctx,
		`SELECT count(*), coalesce(sum(amount), 0)::bigint FROM ledger_entries WHERE account_id = $1 OR account_id = $2`,
		from, to,
	).Scan(&entries, &sum)
	if err != nil {
		t.Fatal(err)
	}
	if entries != 2 || sum != 0 {
		t.Errorf("záznamů = %d, součet = %d, čekám 2 a 0", entries, sum)
	}
}

func TestTransferInsufficientFunds(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	from := createAccount(t, pool, 100)
	to := createAccount(t, pool, 0)

	_, err := Transfer(ctx, pool, uuid.NewString(), from, to, 101)
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("chyba = %v, čekám ErrInsufficientFunds", err)
	}

	if got := getBalance(t, pool, from); got != 100 {
		t.Errorf("zůstatek from = %d, čekám 100", got)
	}
}

func TestTransferWritesOutboxEvent(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	from := createAccount(t, pool, 1000)
	to := createAccount(t, pool, 0)
	idempotencyKey := uuid.NewString()

	result, err := Transfer(ctx, pool, idempotencyKey, from, to, 300)
	if err != nil {
		t.Fatal(err)
	}

	_, err = Transfer(ctx, pool, idempotencyKey, from, to, 300)
	if err != nil {
		t.Fatal(err)
	}

	var events int64
	err = pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox_events WHERE payload->>'transferId' = $1`,
		result.TransferID,
	).Scan(&events)
	if err != nil {
		t.Fatal(err)
	}
	if events != 1 {
		t.Errorf("eventů = %d, čekám 1", events)
	}
}
