package dqlite_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"testing"

	dqlite "github.com/canonical/go-dqlite"
	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/go-dqlite/internal/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNode_Leader(t *testing.T) {
	server, cleanup := newNode(t)
	defer cleanup()

	leader, err := server.Leader(context.Background())
	require.NoError(t, err)

	assert.Equal(t, leader.ID, uint64(1))
	assert.Equal(t, leader.Address, "1")
}

func TestNode_Cluster(t *testing.T) {
	server, cleanup := newNode(t)
	defer cleanup()

	servers, err := server.Cluster(context.Background())
	require.NoError(t, err)

	assert.Len(t, servers, 1)
	assert.Equal(t, servers[0].ID, uint64(1))
	assert.Equal(t, servers[0].Address, "1")
}

// Create a new in-memory server store populated with the given addresses.
func newStore(t *testing.T, address string) *client.DatabaseNodeStore {
	t.Helper()

	store, err := client.DefaultNodeStore(":memory:")
	require.NoError(t, err)

	server := client.NodeInfo{Address: address}
	require.NoError(t, store.Set(context.Background(), []client.NodeInfo{server}))

	return store
}

func dialFunc(ctx context.Context, address string) (net.Conn, error) {
	return net.Dial("unix", fmt.Sprintf("@dqlite-%s", address))
}

func newNode(t *testing.T) (*dqlite.Node, func()) {
	t.Helper()
	dir, dirCleanup := newDir(t)

	info := client.NodeInfo{ID: uint64(1), Address: "1"}
	server, err := dqlite.NewNode(info, dir, dqlite.WithNodeLogFunc(logging.Test(t)))
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
