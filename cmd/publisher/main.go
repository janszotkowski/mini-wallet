package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/janszotkowski/mini-wallet/internal/outbox"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	client, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092")) // Připojení ke Kafce.
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ticker := time.NewTicker(time.Second) // Každou sekundu pošle signál do channelu ticker.C. Funguje podobně jako setInterval. ticker.Stop() odpovídá clearInterval.
	defer ticker.Stop()

	log.Println("publisher started")
	for {
		select {
		case <-ctx.Done():
			log.Println("publisher stopped")
			return
		case <-ticker.C:
			published, err := outbox.PublishPending(ctx, pool, client)
			if err != nil {
				log.Printf("publish pending events: %v", err)
				continue
			}
			if published > 0 {
				log.Printf("published %d events", published)
			}
		}
	}
}
