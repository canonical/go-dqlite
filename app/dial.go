package app

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/canonical/go-dqlite/client"
	"github.com/pkg/errors"
)

// Like client.DialFuncWithTLS but also starts the proxy, since the raft
// connect function only supports Unix and TCP connections.
func makeNodeDialFunc(config *tls.Config) client.DialFunc {
	dial := func(ctx context.Context, addr string) (net.Conn, error) {
		clonedConfig := config.Clone()
		if len(clonedConfig.ServerName) == 0 {

			remoteIP, _, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			clonedConfig.ServerName = remoteIP
		}
		dialer := &net.Dialer{}
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}
		goUnix, cUnix, err := socketpair()
		if err != nil {
			return nil, errors.Wrap(err, "create pair of Unix sockets")
		}

		go proxy(context.Background(), conn, goUnix, clonedConfig)

		return cUnix, nil
	}

	return dial
}
