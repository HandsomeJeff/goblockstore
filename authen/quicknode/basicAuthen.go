package authen

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func GetClientWithBasicAuth(endpoint, token string) (*grpc.ClientConn, error) {
	target := endpoint + ".solana-mainnet.quiknode.pro:10000"
	conn, err := grpc.Dial(target,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})),
		grpc.WithPerRPCCredentials(basicAuth{
			username: endpoint,
			password: token,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to dial endpoint: %w", err)
	}
	return conn, nil
}

type basicAuth struct {
	username string
	password string
}

func (b basicAuth) GetRequestMetadata(ctx context.Context, in ...string) (map[string]string, error) {
	auth := b.username + ":" + b.password
	encoded := base64.StdEncoding.EncodeToString([]byte(auth))
	return map[string]string{"authorization": "Basic " + encoded}, nil
}

func (basicAuth) RequireTransportSecurity() bool {
	return true
}
