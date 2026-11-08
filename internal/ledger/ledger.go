package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrInvalidAmount        = errors.New("amount must be positive") // Velké písmeno na začátku znamená, že je název vidět i mimo balíček, jako export.
	ErrSameAccount          = errors.New("cannot transfer to the same account")
	ErrInsufficientFunds    = errors.New("insufficient funds")
	ErrAccountNotFound      = errors.New("account not found")
	ErrIdempotencyKeyReused = errors.New("idempotency key was already used with different parameters")
)

// type TransferResult = { transferId: string; replayed: boolean }
type TransferResult struct {
	TransferID string
	Replayed   bool
}

type transferCompletedEvent struct {
	TransferID    string `json:"transferId"`
	FromAccountID string `json:"fromAccountId"`
	ToAccountID   string `json:"toAccountId"`
	Amount        int64  `json:"amount"`
}

// int64 = celé číslo.

// ctx = signál pro zrušení práce, obdoba AbortSignal.
// pool = odkaz na sadu připojení k databázi. Hvězdička znamená ukazatel: všichni pracují se stejným poolem, nikdo nemá kopii.
func Transfer(ctx context.Context, pool *pgxpool.Pool, idempotencyKey, fromAccountID, toAccountID string, amount int64) (TransferResult, error) {
	if amount <= 0 {
		return TransferResult{}, ErrInvalidAmount
	}
	if fromAccountID == toAccountID {
		return TransferResult{}, ErrSameAccount
	}

	tx, err := pool.Begin(ctx) // Začne transakci -> tx. Všechno v ní se buď provede celé, nebo vůbec.
	if err != nil {
		return TransferResult{}, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) // Podobá se finally v TS, jen ho píšeš hned vedle věci, kterou má uklidit.

	// Exec vraci CommandTag (shrnutí, třeba UPDATE 1), muze se logovat, a druhy parametr je err, ktery se vraci
	_, err = tx.Exec(ctx,
		`SELECT id FROM accounts WHERE id IN ($1, $2) ORDER BY id FOR UPDATE`,
		fromAccountID, toAccountID,
	)
	if err != nil {
		return TransferResult{}, fmt.Errorf("lock accounts: %w", err)
	}

	var fromBalance int64                                                                                  // Vytvoří proměnnou s výchozí hodnotou 0. V Go má každý typ svou výchozí hodnotu.
	err = tx.QueryRow(ctx, `SELECT balance FROM accounts WHERE id = $1`, fromAccountID).Scan(&fromBalance) // Scan uloží výsledek do proměnné.
	if errors.Is(err, pgx.ErrNoRows) {
		return TransferResult{}, ErrAccountNotFound
	}
	if err != nil {
		return TransferResult{}, fmt.Errorf("get from account: %w", err)
	}

	var toBalance int64
	err = tx.QueryRow(ctx, `SELECT balance FROM accounts WHERE id = $1`, toAccountID).Scan(&toBalance)
	if errors.Is(err, pgx.ErrNoRows) {
		return TransferResult{}, ErrAccountNotFound
	}
	if err != nil {
		return TransferResult{}, fmt.Errorf("get to account: %w", err)
	}

	var transferID string
	err = tx.QueryRow(ctx,
		`INSERT INTO transfers (idempotency_key, from_account_id, to_account_id, amount)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (idempotency_key) DO NOTHING
		 RETURNING id::text`,
		idempotencyKey, fromAccountID, toAccountID, amount,
	).Scan(&transferID)
	if errors.Is(err, pgx.ErrNoRows) {
		return findExistingTransfer(ctx, tx, idempotencyKey, fromAccountID, toAccountID, amount)
	}
	if err != nil {
		return TransferResult{}, fmt.Errorf("insert transfer: %w", err)
	}

	if fromBalance < amount {
		return TransferResult{}, ErrInsufficientFunds
	}

	// Exec provede SQL, které nevrací řádky. První návratovou hodnotu nepotřebujeme a _ ji zahodí.
	_, err = tx.Exec(ctx, `UPDATE accounts SET balance = $1 WHERE id = $2`, fromBalance-amount, fromAccountID)
	if err != nil {
		return TransferResult{}, fmt.Errorf("update from account: %w", err)
	}

	_, err = tx.Exec(ctx, `UPDATE accounts SET balance = $1 WHERE id = $2`, toBalance+amount, toAccountID)
	if err != nil {
		return TransferResult{}, fmt.Errorf("update to account: %w", err)
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO ledger_entries (transfer_id, account_id, amount) 
		 VALUES ($1, $2, $3), ($1, $4, $5)`,
		transferID, fromAccountID, -amount, toAccountID, amount,
	)
	if err != nil {
		return TransferResult{}, fmt.Errorf("insert ledger entries: %w", err)
	}

	payload, err := json.Marshal(transferCompletedEvent{ // Převede struct na JSON, jako JSON.stringify(...). Vrátí []byte (pole bajtů, tedy text JSONu) a chybu.
		TransferID:    transferID,
		FromAccountID: fromAccountID,
		ToAccountID:   toAccountID,
		Amount:        amount,
	})
	if err != nil {
		return TransferResult{}, fmt.Errorf("marshal transfer completed event: %w", err)
	}

	_, err = tx.Exec(ctx, // Zapíše event přes tx, ne přes pool. Tím je ve stejné transakci jako převod. To je celé jádro outboxu.
		`INSERT INTO outbox_events (event_type, payload) VALUES ('transfer.completed', $1)`,
		payload,
	)
	if err != nil {
		return TransferResult{}, fmt.Errorf("insert outbox event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return TransferResult{}, fmt.Errorf("commit transaction: %w", err)
	}

	return TransferResult{TransferID: transferID}, nil
}

// tx pgx.Tx: dostává transakci, ne pool, aby četla ve stejné transakci.
func findExistingTransfer(ctx context.Context, tx pgx.Tx, idempotencyKey, fromAccountID, toAccountID string, amount int64) (TransferResult, error) {
	var (
		transferID     string
		existingFromID string
		existingToID   string
		existingAmount int64
	)

	err := tx.QueryRow(ctx,
		`SELECT id::text, from_account_id::text, to_account_id::text, amount
		 FROM transfers
		 WHERE idempotency_key = $1`,
		idempotencyKey,
	).Scan(&transferID, &existingFromID, &existingToID, &existingAmount)
	if err != nil {
		return TransferResult{}, fmt.Errorf("find existing transfer: %w", err)
	}

	if existingFromID != fromAccountID || existingToID != toAccountID || existingAmount != amount {
		return TransferResult{}, ErrIdempotencyKeyReused
	}

	return TransferResult{TransferID: transferID, Replayed: true}, nil
}
