package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx := context.Background() // vytvoreni contextu

	databaseURL := os.Getenv("DATABASE_URL") // vytahnuti promenne z .env souboru, pokud chybi vrati se "" misto undefined
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is not set") // pokud chybi, vrati se chyba a program se ukonci
	}

	pool, err := pgxpool.New(ctx, databaseURL) // vytvoreni poolu (sada pripojeni) pro pripojeni k databazi, funkce vrati error pokud se nepovede vytvorit pool
	// Pool potřebujeme, protože Go obsluhuje každý HTTP požadavek ve vlastní goroutine, takže jich může běžet víc současně. Každý si pro dotaz půjčí jedno připojení a pak ho vrátí.
	if err != nil { // nil je obdoba null
		log.Fatal(err)
	}

	mux := http.NewServeMux() // router

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		// Ping je funkce pro kontrolu, zda je databaze dostupna. Pokud ne, vrati error.
		if err := pool.Ping(r.Context()); err != nil {
			// Pošle odpověď se statusem 503 a tímto textem.
			http.Error(w, "Database unavailable", http.StatusServiceUnavailable)
			return // pokud se stane chyba, funkce se ukonci a odpoved se neposle
		}	
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	log.Println("Starting server on port 8080")
	err = http.ListenAndServe(":8080", mux) // Je tu =, ne :=, protože err už existuje. := vytváří novou proměnnou, = jen mění existující.
	log.Fatal(err)
}
