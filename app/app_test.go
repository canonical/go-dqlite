package app_test

import (
	"context"
	"fmt"
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
	_, cleanup := newApp(t)
	defer cleanup()
}

// Create a pristine joining node.
func TestNew_PristineJoiner(t *testing.T) {
	addr1 := "127.0.0.1:9001"
	addr2 := "127.0.0.1:9002"

	app1, cleanup := newApp(t, app.WithAddress(addr1))
	defer cleanup()

	cli, err := app1.Leader(context.Background())
	require.NoError(t, err)

	id := dqlite.GenerateID(addr2)
	err = cli.Add(context.Background(), client.NodeInfo{ID: id, Address: addr2, Role: client.Spare})
	require.NoError(t, err)

	app2, cleanup := newApp(t, app.WithID(id), app.WithAddress(addr2), app.WithCluster([]string{addr1}))
	defer cleanup()

	err = cli.Assign(context.Background(), id, client.Voter)
	require.NoError(t, err)
}

// Open a database on a fresh one-node cluster.
func TestOpen(t *testing.T) {
	app, cleanup := newApp(t)
	defer cleanup()

	db, err := app.Open(context.Background(), "test")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.ExecContext(context.Background(), "CREATE TABLE foo(n INT)")
	assert.NoError(t, err)
}

func newApp(t *testing.T, options ...app.Option) (*app.App, func()) {
	t.Helper()

	dir, dirCleanup := newDir(t)

	log := func(l client.LogLevel, format string, a ...interface{}) {
		format = fmt.Sprintf("%s: %s", l.String(), format)
		t.Logf(format, a...)
	}

	options = append(options, app.WithLogFunc(log))

	app, err := app.New(dir, options...)
	require.NoError(t, err)

	cleanup := func() {
		require.NoError(t, app.Close())
		dirCleanup()
	}

	return app, cleanup
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
