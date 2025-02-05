package main

import (
	// "context"
	"context"
	"crypto/tls"

	"fmt"
	"log"
	"os"
	"time"

	// "encoding/json"
	// "github.com/mr-tron/base58"

	pb "goblockstore/proto" // proto directory path

	"github.com/joho/godotenv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/encoding/gzip"
)

// var (
// 	endpoint string
// 	token    string
// )

// var kacp = keepalive.ClientParameters{
// 	Time:                10 * time.Second,
// 	Timeout:             time.Second,
// 	PermitWithoutStream: true,
// }

// // tokenAuth implements the credentials.PerRPCCredentials interface
// type tokenAuth struct {
// 	token string
// }

// func (t tokenAuth) GetRequestMetadata(ctx context.Context, in ...string) (map[string]string, error) {
// 	return map[string]string{"x-token": t.token}, nil
// }

// func (tokenAuth) RequireTransportSecurity() bool {
// 	return true
// }

func test_conn() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	endpoint = os.Getenv("QUICKNODE_ENDPOINT")
	token = os.Getenv("QUICKNODE_TOKEN")

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})),
		grpc.WithKeepaliveParams(kacp),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024*1024*1024), grpc.UseCompressor(gzip.Name)),
		grpc.WithPerRPCCredentials(tokenAuth{token: token}),
	}
	conn, err := grpc.Dial(endpoint, opts...)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewGeyserClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	latestBlockHash, err := client.GetLatestBlockhash(ctx, &pb.GetLatestBlockhashRequest{})
	if err != nil {
		log.Fatalf("Failed to get latest blockhash: %v", err)
	}

	fmt.Printf("Latest Blockhash Information: ")
	fmt.Printf("  Blockhash: %+v", latestBlockHash)
}
