package app_test

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/canonical/go-dqlite/app"
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
