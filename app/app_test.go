package app_test

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/canonical/go-dqlite"
	"github.com/canonical/go-dqlite/app"
	"github.com/canonical/go-dqlite/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Create a pristine bootstrap node with default value.
func TestNew_PristineDefault(t *testing.T) {
	dir, cleanup := newDir(t)
	defer cleanup()

	app, err := app.New(dir)
	assert.NoError(t, err)
	assert.NoError(t, app.Close())
}

// Create a pristine joining node.
func TestNew_PristineJoiner(t *testing.T) {
	dir1, cleanup := newDir(t)
	defer cleanup()

	dir2, cleanup := newDir(t)
	defer cleanup()

	addr1 := "127.0.0.1:9001"
	addr2 := "127.0.0.1:9002"

	app1, err := app.New(dir1, app.WithAddress(addr1))
	assert.NoError(t, err)
	defer app1.Close()

	cli, err := app1.Leader(context.Background())
	require.NoError(t, err)

	id := dqlite.GenerateID(addr2)
	err = cli.Add(context.Background(), client.NodeInfo{ID: id, Address: addr2, Role: client.Spare})
	require.NoError(t, err)

	app2, err := app.New(dir2, app.WithID(id), app.WithAddress(addr2), app.WithCluster([]string{addr1}))
	require.NoError(t, err)
	defer app2.Close()

	err = cli.Assign(context.Background(), id, client.Voter)
	require.NoError(t, err)
}

// Open a database on a fresh one-node cluster.
func TestOpen(t *testing.T) {
	dir, cleanup := newDir(t)
	defer cleanup()

	app, err := app.New(dir)
	assert.NoError(t, err)
	defer app.Close()

	db, err := app.Open(context.Background(), "test")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.ExecContext(context.Background(), "CREATE TABLE foo(n INT)")
	assert.NoError(t, err)
}

// Return a new temporary directory.
func newDir(t *testing.T) (string, func()) {
	t.Helper()

	dir, err := ioutil.TempDir("", "dqlite-app-test-")
	assert.NoError(t, err)

	cleanup := func() {
		os.RemoveAll(dir)
	}

	return dir, cleanup
}
