package dqlite_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/Rican7/retry/backoff"
	"github.com/Rican7/retry/strategy"
	dqlite "github.com/canonical/go-dqlite"
	"github.com/canonical/go-dqlite/internal/client"
	"github.com/canonical/go-dqlite/internal/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServer_Dump(t *testing.T) {
	server, cleanup := newServer(t)
	defer cleanup()

	store := newStore(t, "1")
	config := client.Config{
		Dial:           dialFunc,
		AttemptTimeout: 100 * time.Millisecond,
		RetryStrategies: []strategy.Strategy{
			strategy.Backoff(backoff.BinaryExponential(time.Millisecond)),
		},
	}

	log := func(l logging.Level, format string, a ...interface{}) {
		format = fmt.Sprintf("%s: %s", l.String(), format)
		t.Logf(format, a...)
	}

	connector := client.NewConnector(0, store, config, log)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	c, err := connector.Connect(ctx)
	require.NoError(t, err)
	defer c.Close()

	// Open a database and create a test table.
	request := client.Message{}
	request.Init(4096)

	response := client.Message{}
	response.Init(4096)

	client.EncodeOpen(&request, "test.db", 0, "volatile")

	err = c.Call(ctx, &request, &response)
	require.NoError(t, err)

	db, err := client.DecodeDb(&response)
	require.NoError(t, err)

	request.Reset()
	response.Reset()

	client.EncodeExecSQL(&request, uint64(db), "CREATE TABLE foo (n INT)", nil)

	err = c.Call(ctx, &request, &response)
	require.NoError(t, err)

	request.Reset()
	response.Reset()

	files, err := server.Dump(ctx, "test.db")
	require.NoError(t, err)

	require.Len(t, files, 2)
	assert.Equal(t, "test.db", files[0].Name)
	assert.Equal(t, 4096, len(files[0].Data))

	assert.Equal(t, "test.db-wal", files[1].Name)
	assert.Equal(t, 8272, len(files[1].Data))
}

func TestServer_LeaderAddress(t *testing.T) {
	server, cleanup := newServer(t)
	defer cleanup()

	leader, err := server.LeaderAddress(context.Background())
	require.NoError(t, err)

	assert.Equal(t, "1", leader)
}

// Create a new in-memory server store populated with the given addresses.
func newStore(t *testing.T, address string) *dqlite.DatabaseServerStore {
	t.Helper()

	store, err := dqlite.DefaultServerStore(":memory:")
	require.NoError(t, err)

	server := dqlite.ServerInfo{Address: address}
	require.NoError(t, store.Set(context.Background(), []dqlite.ServerInfo{server}))

	return store
}

func newServer(t *testing.T) (*dqlite.Server, func()) {
	t.Helper()
	dir, dirCleanup := newDir(t)

	info := dqlite.ServerInfo{ID: uint64(1), Address: "1"}
	server, err := dqlite.NewServer(info, dir, dqlite.WithServerLogFunc(logging.Test(t)))
	require.NoError(t, err)

	err = server.Start()
	require.NoError(t, err)

	cleanup := func() {
		require.NoError(t, server.Close())
		dirCleanup()
	}

	return server, cleanup
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
