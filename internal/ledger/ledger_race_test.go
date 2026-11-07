package ledger

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/google/uuid"
)

func TestTransferConcurrent(t *testing.T) {
	const (
		accountCount   = 5
		initialBalance = int64(1000)
		transferCount  = 100
		transferAmount = int64(10)
	)

	ctx := context.Background()
	pool := newTestPool(t)

	accounts := make([]string, 0, accountCount)
	for range accountCount {
		accounts = append(accounts, createAccount(t, pool, initialBalance))
	}

	var wg sync.WaitGroup // Počítadlo spuštěných úloh, na které lze počkat. Podobá se Promise.all
	for range transferCount {
		wg.Go(func() { // Spustí funkci v nové goroutine, tedy lehkém vlákně běžícím souběžně s ostatními. goroutines můžou běžet opravdu současně na více jádrech procesoru.
			fromIndex := rand.IntN(accountCount)                                  // Náhodný účet odesílatele, číslo od 0 do 4.
			toIndex := (fromIndex + 1 + rand.IntN(accountCount-1)) % accountCount // Náhodný příjemce, který nikdy není stejný jako odesílatel.

			_, err := Transfer(ctx, pool, uuid.NewString(), accounts[fromIndex], accounts[toIndex], transferAmount)
			if err != nil && !errors.Is(err, ErrInsufficientFunds) {
				t.Errorf("převod selhal: %v", err)
			}
		})
	}
	wg.Wait() // Počká, až doběhnou všechny převody, jako await Promise.all(...).

	var total int64
	for _, id := range accounts {
		balance := getBalance(t, pool, id)
		total += balance

		var ledgerSum int64
		err := pool.QueryRow(ctx,
			`SELECT coalesce(sum(amount), 0)::bigint FROM ledger_entries WHERE account_id = $1`,
			id,
		).Scan(&ledgerSum)
		if err != nil {
			t.Fatal(err)
		}

		if balance != initialBalance+ledgerSum {
			t.Errorf("účet %s: zůstatek = %d, podle ledgeru má být %d", id, balance, initialBalance+ledgerSum)
		}
	}

	want := initialBalance * accountCount
	if total != want {
		t.Errorf("součet zůstatků = %d, čekám %d", total, want)
	}
}
