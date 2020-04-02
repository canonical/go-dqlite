package app_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/canonical/go-dqlite/app"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	dir, cleanup := newDir(t)
	defer cleanup()

	app, err := app.New(dir)
	assert.NoError(t, err)
	assert.NoError(t, app.Close())
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
