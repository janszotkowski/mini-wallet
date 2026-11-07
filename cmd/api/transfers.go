package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/janszotkowski/mini-wallet/internal/ledger"
)

type createTransferRequest struct {
	FromAccountID string `json:"fromAccountId"`
	ToAccountID   string `json:"toAccountId"`
	Amount        int64  `json:"amount"`
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}

// Funkce vraci funkci - mux.HandleFunc potřebuje funkci s přesně dvěma parametry, w a r. Handler ale potřebuje i pool, a ten mu jako třetí parametr předat nejde.
func handleCreateTransfer(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var request createTransferRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil { // Přečte JSON z těla do request
			writeError(w, http.StatusBadRequest, "invalid JSON body")
		}

		// Ověří, že ID je platné UUID
		fromAccountID, err := uuid.Parse(request.FromAccountID)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid fromAccountId")
			return
		}

		toAccountID, err := uuid.Parse(request.ToAccountID)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid toAccountId")
			return
		}

		err = ledger.Transfer(r.Context(), pool, fromAccountID.String(), toAccountID.String(), request.Amount)
		switch { // Podle chyby vybere HTTP status, break se nepíše
		case err == nil:
			w.WriteHeader(http.StatusNoContent) // 204 No Content
		case errors.Is(err, ledger.ErrInvalidAmount), errors.Is(err, ledger.ErrSameAccount):
			writeError(w, http.StatusBadRequest, err.Error()) // 400 Bad Request
		case errors.Is(err, ledger.ErrAccountNotFound):
			writeError(w, http.StatusNotFound, err.Error()) // 404 Not Found
		case errors.Is(err, ledger.ErrInsufficientFunds):
			writeError(w, http.StatusUnprocessableEntity, err.Error()) //422 Unprocessable Entity
		default:
			log.Printf("create transfer: %v", err)
			writeError(w, http.StatusInternalServerError, "internal server error") // 500 Internal Server Error
		}
	}
}
