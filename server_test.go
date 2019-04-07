package dqlite_test

import (
	"context"
	"io/ioutil"
	"net"
	"os"
	"testing"

	dqlite "github.com/CanonicalLtd/go-dqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// func TestServer_Dump(t *testing.T) {
// 	// Start a server and connect to it.
// 	listener := newListener(t)
// 	server, cleanup := newServer(t, listener)
// 	defer cleanup()

// 	address := listener.Addr().String()
// 	store := newStore(t, address)
// 	config := client.Config{
// 		Dial:           client.TCPDial,
// 		AttemptTimeout: 100 * time.Millisecond,
// 		RetryStrategies: []strategy.Strategy{
// 			strategy.Backoff(backoff.BinaryExponential(time.Millisecond)),
// 		},
// 	}

// 	log := func(l logging.Level, format string, a ...interface{}) {
// 		format = fmt.Sprintf("%s: %s", l.String(), format)
// 		t.Logf(format, a...)
// 	}

// 	connector := client.NewConnector(0, store, config, log)

// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
// 	defer cancel()

// 	c, err := connector.Connect(ctx)
// 	require.NoError(t, err)

// 	// Open a database and create a test table.
// 	request := client.Message{}
// 	request.Init(4096)

// 	response := client.Message{}
// 	response.Init(4096)

// 	flags := uint64(bindings.OpenReadWrite | bindings.OpenCreate)
// 	client.EncodeOpen(&request, "test.db", flags, "volatile")

// 	err = c.Call(ctx, &request, &response)
// 	require.NoError(t, err)

// 	db, err := client.DecodeDb(&response)
// 	require.NoError(t, err)

// 	request.Reset()
// 	response.Reset()

// 	client.EncodeExecSQL(&request, uint64(db), "CREATE TABLE foo (n INT)", nil)

// 	err = c.Call(ctx, &request, &response)
// 	require.NoError(t, err)

// 	request.Reset()
// 	response.Reset()

// 	// Dump the database to disk.
// 	dir, err := ioutil.TempDir("", "dqlite-server-")
// 	require.NoError(t, err)

// 	defer os.RemoveAll(dir)

// 	err = server.Dump("test.db", dir)
// 	require.NoError(t, err)

// 	require.NoError(t, c.Close())
// }

// Create a new in-memory server store populated with the given addresses.
func newStore(t *testing.T, address string) *dqlite.DatabaseServerStore {
	t.Helper()

	store, err := dqlite.DefaultServerStore(":memory:")
	require.NoError(t, err)

	server := dqlite.ServerInfo{Address: address}
	require.NoError(t, store.Set(context.Background(), []dqlite.ServerInfo{server}))

	return store
}

func newServer(t *testing.T, listener net.Listener) (*dqlite.Server, func()) {
	t.Helper()
	dir, dirCleanup := newDir(t)

	info := dqlite.ServerInfo{ID: uint64(1), Address: listener.Addr().String()}
	server, err := dqlite.NewServer(info, dir)
	require.NoError(t, err)

	servers := []dqlite.ServerInfo{
		{ID: 1, Address: listener.Addr().String()},
	}
	err = server.Bootstrap(servers)
	require.NoError(t, err)

	err = server.Start(listener)
	require.NoError(t, err)

	cleanup := func() {
		require.NoError(t, server.Close())
		dirCleanup()
	}

	return server, cleanup
}

func newListener(t *testing.T) net.Listener {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	return listener
}

// Return a new temporary directory.
func newDir(t *testing.T) (string, func()) {
	t.Helper()

	dir, err := ioutil.TempDir("", "dqlite-replication-test-")
	assert.NoError(t, err)

	cleanup := func() {
		_, err := os.Stat(dir)
		if err != nil {
			assert.True(t, os.IsNotExist(err))
		} else {
			assert.NoError(t, os.RemoveAll(dir))
		}
	}

	return dir, cleanup
}
