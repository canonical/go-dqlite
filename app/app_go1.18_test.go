//go:build go1.18
// +build go1.18

package app_test

// import (
// 	"context"
// 	"crypto/tls"
// 	"net"
// 	"testing"

// 	"github.com/canonical/go-dqlite/app"
// 	"github.com/canonical/go-dqlite/client"
// 	"github.com/quic-go/quic-go"
// 	"github.com/stretchr/testify/assert"
// 	"github.com/stretchr/testify/require"
// )

// // quic.Stream doesn't implement net.Conn, so we need to wrap it.
// type quicConn struct {
// 	quic.Stream
// }

// func (c *quicConn) LocalAddr() net.Addr {
// 	return nil
// }

// func (c *quicConn) RemoteAddr() net.Addr {
// 	return nil
// }

// // TestExternalConnWithQUIC creates a 3-member cluster using external quic connection
// // and ensures the cluster is successfully created, and that the connection is
// // handled manually.
// func TestExternalConnWithQUIC(t *testing.T) {
// 	externalAddr1 := "127.0.0.1:9191"
// 	externalAddr2 := "127.0.0.1:9292"
// 	externalAddr3 := "127.0.0.1:9393"
// 	acceptCh1 := make(chan net.Conn)
// 	acceptCh2 := make(chan net.Conn)
// 	acceptCh3 := make(chan net.Conn)

// 	dialFunc := func(ctx context.Context, addr string) (net.Conn, error) {
// 		conn, err := quic.DialAddrContext(ctx, addr, &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"quic"}}, nil)
// 		require.NoError(t, err)

// 		stream, err := conn.OpenStreamSync(ctx)
// 		require.NoError(t, err)

// 		return &quicConn{
// 			Stream: stream,
// 		}, nil
// 	}

// 	cert, pool := loadCert(t)
// 	tlsconfig := app.SimpleListenTLSConfig(cert, pool)
// 	tlsconfig.NextProtos = []string{"quic"}
// 	tlsconfig.ClientAuth = tls.NoClientCert

// 	serveQUIC := func(addr string, acceptCh chan net.Conn, cleanups chan func()) {
// 		lis, err := quic.ListenAddr(addr, tlsconfig, nil)
// 		require.NoError(t, err)

// 		ctx, cancel := context.WithCancel(context.Background())

// 		go func() {
// 			for {
// 				select {
// 				case <-ctx.Done():
// 					return
// 				default:
// 					conn, err := lis.Accept(context.Background())
// 					if err != nil {
// 						return
// 					}

// 					stream, err := conn.AcceptStream(context.Background())
// 					if err != nil {
// 						return
// 					}

// 					acceptCh <- &quicConn{
// 						Stream: stream,
// 					}
// 				}
// 			}
// 		}()

// 		cleanup := func() {
// 			cancel()
// 			require.NoError(t, lis.Close())
// 		}

// 		cleanups <- cleanup
// 	}

// 	liscleanups := make(chan func(), 3)
// 	// Start up three listeners.
// 	go serveQUIC(externalAddr1, acceptCh1, liscleanups)
// 	go serveQUIC(externalAddr2, acceptCh2, liscleanups)
// 	go serveQUIC(externalAddr3, acceptCh3, liscleanups)

// 	defer func() {
// 		for i := 0; i < 3; i++ {
// 			cleanup := <-liscleanups
// 			cleanup()
// 		}
// 		close(liscleanups)
// 	}()

// 	app1, cleanup := newAppWithNoTLS(t, app.WithAddress(externalAddr1), app.WithExternalConn(dialFunc, acceptCh1))
// 	defer cleanup()

// 	app2, cleanup := newAppWithNoTLS(t, app.WithAddress(externalAddr2), app.WithExternalConn(dialFunc, acceptCh2), app.WithCluster([]string{externalAddr1}))
// 	defer cleanup()

// 	require.NoError(t, app2.Ready(context.Background()))

// 	app3, cleanup := newAppWithNoTLS(t, app.WithAddress(externalAddr3), app.WithExternalConn(dialFunc, acceptCh3), app.WithCluster([]string{externalAddr1}))
// 	defer cleanup()

// 	require.NoError(t, app3.Ready(context.Background()))

// 	// Get a client from the first node (likely the leader).
// 	cli, err := app1.Leader(context.Background())
// 	require.NoError(t, err)
// 	defer cli.Close()

// 	// Ensure entries exist for each cluster member.
// 	cluster, err := cli.Cluster(context.Background())
// 	require.NoError(t, err)
// 	assert.Equal(t, externalAddr1, cluster[0].Address)
// 	assert.Equal(t, externalAddr2, cluster[1].Address)
// 	assert.Equal(t, externalAddr3, cluster[2].Address)

// 	// Every cluster member should be a voter.
// 	assert.Equal(t, client.Voter, cluster[0].Role)
// 	assert.Equal(t, client.Voter, cluster[1].Role)
// 	assert.Equal(t, client.Voter, cluster[2].Role)
// }
