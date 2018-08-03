package bindings_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/CanonicalLtd/go-dqlite/internal/bindings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// When a database is in WAL mode, enabling the NO_CKPT_ON_CLOSE option
// prevents the WAL from being checkpointed when the connection is closed.
func TestConn_ConfigNoCkptOnClose(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	dir, err := ioutil.TempDir("", "go-dqlite-bindings-")
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	name := filepath.Join(dir, "test.db")
	vfs := "unix"

	conn, err := bindings.Open(name, vfs)
	require.NoError(t, err)

	// Switch on the flag.
	flag, err := conn.ConfigNoCkptOnClose(true)
	require.NoError(t, err)

	assert.True(t, flag)

	// Enable WAL mode and write something.
	err = conn.Exec("PRAGMA journal_mode=wal; CREATE TABLE foo (n INT)")
	require.NoError(t, err)

	// The WAL file got created.
	wal := conn.Filename() + "-wal"
	_, err = os.Stat(wal)
	require.NoError(t, err)

	conn.Close()

	// The WAL file is still there after closing.
	_, err = os.Stat(wal)
	require.NoError(t, err)

}
