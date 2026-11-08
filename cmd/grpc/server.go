package main

import (
	"context"
	"errors"
	"log"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	walletv1 "github.com/janszotkowski/mini-wallet/gen/wallet/v1"
	"github.com/janszotkowski/mini-wallet/internal/ledger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type walletServer struct {
	walletv1.UnimplementedWalletServiceServer
	pool *pgxpool.Pool
}

func (s *walletServer) CreateTransfer(ctx context.Context, request *walletv1.CreateTransferRequest) (*walletv1.CreateTransferResponse, error) {
	idempotencyKey := request.GetIdempotencyKey()
	if idempotencyKey == "" {
		return nil, status.Error(codes.InvalidArgument, "missing idempotency_key")
	}
	if len(idempotencyKey) > 255 {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key is too long")
	}

	fromAccountID, err := uuid.Parse(request.GetFromAccountId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid from_account_id")
	}

	toAccountID, err := uuid.Parse(request.GetToAccountId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid to_account_id")
	}

	result, err := ledger.Transfer(ctx, s.pool, idempotencyKey, fromAccountID.String(), toAccountID.String(), request.GetAmount())
	switch {
	case err == nil:
		return &walletv1.CreateTransferResponse{TransferId: result.TransferID, Replayed: result.Replayed}, nil
	case errors.Is(err, ledger.ErrInvalidAmount), errors.Is(err, ledger.ErrSameAccount):
		return nil, status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ledger.ErrAccountNotFound):
		return nil, status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ledger.ErrInsufficientFunds), errors.Is(err, ledger.ErrIdempotencyKeyReused):
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	default:
		log.Printf("create transfer: %v", err)
		return nil, status.Error(codes.Internal, "internal error")
	}
}
