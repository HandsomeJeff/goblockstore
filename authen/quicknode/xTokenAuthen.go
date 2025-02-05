package authen

import (
	"context"
	"crypto/tls"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func GetClientWithXToken(endpoint, token string) (*grpc.ClientConn, error) {
	target := endpoint + ".solana-mainnet.quiknode.pro:10000"
	conn, err := grpc.Dial(target,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})),
		grpc.WithPerRPCCredentials(xTokenAuth{
			token: token,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to dial endpoint: %w", err)
	}
	return conn, nil
}

type xTokenAuth struct {
	token string
}

func (x xTokenAuth) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{"x-token": x.token}, nil
}

func (xTokenAuth) RequireTransportSecurity() bool {
	return true
}
