// +build !nosqlite3

package client_test

import (
	"context"
	"database/sql"
	"testing"

	dqlite "github.com/canonical/go-dqlite"
	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/go-dqlite/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Exercise setting and getting servers in a DatabaseNodeStore created with
// DefaultNodeStore.
func TestDefaultNodeStore(t *testing.T) {
	// Create a new default store.
	store, err := client.DefaultNodeStore(":memory:")
	require.NoError(t, err)

	// Set and get some targets.
	err = store.Set(context.Background(), []client.NodeInfo{
		{Address: "1.2.3.4:666"}, {Address: "5.6.7.8:666"}},
	)
	require.NoError(t, err)

	servers, err := store.Get(context.Background())
	assert.Equal(t, []client.NodeInfo{
		{ID: uint64(1), Address: "1.2.3.4:666"},
		{ID: uint64(1), Address: "5.6.7.8:666"}},
		servers)

	// Set and get some new targets.
	err = store.Set(context.Background(), []client.NodeInfo{
		{Address: "1.2.3.4:666"}, {Address: "9.9.9.9:666"},
	})
	require.NoError(t, err)

	servers, err = store.Get(context.Background())
	assert.Equal(t, []client.NodeInfo{
		{ID: uint64(1), Address: "1.2.3.4:666"},
		{ID: uint64(1), Address: "9.9.9.9:666"}},
		servers)

	// Setting duplicate targets returns an error and the change is not
	// persisted.
	err = store.Set(context.Background(), []client.NodeInfo{
		{Address: "1.2.3.4:666"}, {Address: "1.2.3.4:666"},
	})
	assert.EqualError(t, err, "failed to insert server 1.2.3.4:666: UNIQUE constraint failed: servers.address")

	servers, err = store.Get(context.Background())
	assert.Equal(t, []client.NodeInfo{
		{ID: uint64(1), Address: "1.2.3.4:666"},
		{ID: uint64(1), Address: "9.9.9.9:666"}},
		servers)
}

func TestConfigMultiThread(t *testing.T) {
	cleanup := dummyDBSetup(t)
	defer cleanup()

	err := dqlite.ConfigMultiThread()
	assert.EqualError(t, err, "SQLite is already initialized")
}

func dummyDBSetup(t *testing.T) func() {
	store := client.NewInmemNodeStore()
	driver, err := driver.New(store)
	require.NoError(t, err)
	sql.Register("dummy", driver)
	db, err := sql.Open("dummy", "test.db")
	require.NoError(t, err)
	cleanup := func() {
		require.NoError(t, db.Close())
	}
	return cleanup
}
