package app

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"

	"github.com/canonical/go-dqlite/client"
)

// Like client.DialFuncWithTLS but also starts the proxy, since the raft
// connect function only supports Unix and TCP connections.
func makeNodeDialFunc(appCtx context.Context, config *tls.Config) client.DialFunc {
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
			return nil, fmt.Errorf("create pair of Unix sockets: %w", err)
		}

		go proxy(appCtx, conn, goUnix, clonedConfig)

		return cUnix, nil
	}

	return dial
}

// extDialFuncWithProxy executes given DialFunc and then copies the data back
// and forth between the remote connection and a local unix socket.
func extDialFuncWithProxy(appCtx context.Context, dialFunc client.DialFunc) client.DialFunc {
	return func(ctx context.Context, addr string) (net.Conn, error) {
		goUnix, cUnix, err := socketpair()
		if err != nil {
			return nil, fmt.Errorf("create pair of Unix sockets: %w", err)
		}

		conn, err := dialFunc(ctx, addr)
		if err != nil {
			return nil, err
		}

		go proxy(appCtx, conn, goUnix, nil)

		return cUnix, nil
	}
}
