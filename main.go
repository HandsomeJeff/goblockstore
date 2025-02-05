package main

import (
	"context"
	"crypto/tls"
	"log"
	"os"
	"time"

	"goblockstore/db"
	"goblockstore/parser"
	pb "goblockstore/proto"

	"github.com/joho/godotenv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/keepalive"
)

// QuickNode endpoints consist of two crucial components: the endpoint name and the corresponding token
// For eg: QN Endpoint: https://docs-demo.solana-mainnet.quiknode.pro/abcde123456789
// endpoint will be: docs-demo.solana-mainnet.quiknode.pro:10000  {10000 is the port number for gRPC}
// token will be : abcde123456789

var (
	endpoint string
	token    string
)

var kacp = keepalive.ClientParameters{
	Time:                10 * time.Second,
	Timeout:             time.Second,
	PermitWithoutStream: true,
}

// tokenAuth implements the credentials.PerRPCCredentials interface
type tokenAuth struct {
	token string
}

func (t tokenAuth) GetRequestMetadata(ctx context.Context, in ...string) (map[string]string, error) {
	return map[string]string{"x-token": t.token}, nil
}

func (tokenAuth) RequireTransportSecurity() bool {
	return true
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Connect to SingleStore
	dbConn, err := db.Connect()
	if err != nil {
		log.Fatalf("Failed to connect to SingleStore: %v", err)
	}
	defer dbConn.Close()

	// Connect to Solana gRPC
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

	commitment := pb.CommitmentLevel_FINALIZED
	subReq := &pb.SubscribeRequest{
		Commitment: &commitment,
		Blocks: map[string]*pb.SubscribeRequestFilterBlocks{
			"blocks": {},
		},
	}

	stream, err := client.Subscribe(context.Background())
	if err != nil {
		log.Fatalf("Failed to subscribe to yellowstone: %v", err)
		return
	}

	if err = stream.Send(subReq); err != nil {
		log.Fatalf("Failed to send subscription request: %v", err)
		return
	}

	log.Println("Listening for blocks...")
	for {
		m, err := stream.Recv()
		if err != nil {
			log.Printf("Failed to receive yellowstone message: %v", err)
			return
		}

		if block := m.GetBlock(); block != nil {

			startTime := time.Now()
			// Parse the block
			parsedBlock, err := parser.ParseBlock(block)
			if err != nil {
				log.Printf("Failed to parse block: %v", err)
				continue
			}
			timeTaken := time.Since(startTime)
			log.Printf("Time taken to parse block: %v, block number: %d, raw tx len: %d, parsed tx len: %d", timeTaken, block.BlockHeight.BlockHeight, len(block.Transactions), len(parsedBlock.Transactions))

			// log.Printf("Parsed block: %v", parsedBlock)

			// Save to database
			if err := parser.SaveToDatabase(dbConn, parsedBlock); err != nil {
				log.Printf("Failed to save block to database: %v", err)
				continue
			}

			log.Printf("Successfully processed block %d with %d transactions", block.Slot, len(parsedBlock.Transactions))
			break
		}
	}
}
