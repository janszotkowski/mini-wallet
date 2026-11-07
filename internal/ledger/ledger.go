package ledger

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrInvalidAmount = errors.New("amount must be positive") // Velké písmeno na začátku znamená, že je název vidět i mimo balíček, jako export.
	ErrSameAccount = errors.New("cannot transfer to the same account")
	ErrInsufficientFunds = errors.New("insufficient funds")
)

func Transfer(ctx context.Context, pool *pgxpool.Pool, fromAccountID, toAccountID string, amount int64) error {
	if amount <= 0 {
		return ErrInvalidAmount
	}
	if fromAccountID == toAccountID {
		return ErrSameAccount
	}

	tx, err := pool.Begin(ctx) // Začne transakci. Všechno v ní se buď provede celé, nebo vůbec.
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) // Podobá se finally v TS, jen ho píšeš hned vedle věci, kterou má uklidit.

	_, err = tx.Exec(ctx,
		`SELECT id FROM accounts WHERE id IN ($1, $2) ORDER BY id FOR UPDATE`,
		fromAccountID, toAccountID,
	)
	if err != nil {
		return fmt.Errorf("lock accounts: %w", err)
	}

	var fromBalance int64 // Vytvoří proměnnou s výchozí hodnotou 0. V Go má každý typ svou výchozí hodnotu.
	err = tx.QueryRow(ctx, `SELECT balance FROM accounts WHERE id = $1`, fromAccountID).Scan(&fromBalance) // Scan uloží výsledek do proměnné.
	if err != nil {
		return fmt.Errorf("get from account: %w", err)
	}

	// Když na účtu není dost peněz, funkce vrátí chybu a transakce se díky defer vrátí zpět.
	if fromBalance < amount {
		return ErrInsufficientFunds
	}

	var toBalance int64
	err = tx.QueryRow(ctx, `SELECT balance FROM accounts WHERE id = $1`, toAccountID).Scan(&toBalance)
	if err != nil {
		return fmt.Errorf("get to account: %w", err)
	}

	// Exec provede SQL, které nevrací řádky. První návratovou hodnotu nepotřebujeme a _ ji zahodí.
	_, err = tx.Exec(ctx, `UPDATE accounts SET balance = $1 WHERE id = $2`, fromBalance-amount, fromAccountID)
	if err != nil {
		return fmt.Errorf("update from account: %w", err)
	}

	_, err = tx.Exec(ctx, `UPDATE accounts SET balance = $1 WHERE id = $2`, toBalance+amount, toAccountID)
	if err != nil {
		return fmt.Errorf("update to account: %w", err)
	}

	var transferID string
	err = tx.QueryRow(ctx, `SELECT uuidv7()::text`).Scan(&transferID)
	if err != nil {
		return fmt.Errorf("generate transfer ID: %w", err)
	}

	_, err = tx.Exec(ctx, 
		`INSERT INTO ledger_entries (transfer_id, account_id, amount) 
		 VALUES ($1, $2, $3), ($1, $4, $5)`, 
		 transferID, fromAccountID, -amount, toAccountID, amount,
	)

	if err != nil {
		return fmt.Errorf("insert ledger entries: %w", err)
	}

	// Commit změny trvale uloží. Do té doby je ostatní transakce nevidí.
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}
