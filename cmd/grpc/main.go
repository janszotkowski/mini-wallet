package main

import (
	"context"
	"log"
	"net"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	walletv1 "github.com/janszotkowski/mini-wallet/gen/wallet/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	pool, err := pgxpool.New(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	listener, err := net.Listen("tcp", ":9090")
	if err != nil {
		log.Fatal(err)
	}

	server := grpc.NewServer()
	walletv1.RegisterWalletServiceServer(server, &walletServer{pool: pool})
	reflection.Register(server)

	log.Println("gRPC server listening on :9090")
	if err := server.Serve(listener); err != nil {
		log.Fatal(err)
	}
}
