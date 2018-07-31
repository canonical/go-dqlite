package bindings_test

import (
	"database/sql/driver"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/CanonicalLtd/go-dqlite/internal/bindings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpen_OpenError(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	conn, err := bindings.Open("test.db", "foo")
	assert.Nil(t, conn)
	assert.EqualError(t, err, "no such vfs: foo")

	sqliteErr, ok := err.(bindings.Error)
	assert.True(t, ok)

	assert.Equal(t, 1, sqliteErr.Code)
	assert.Equal(t, "no such vfs: foo", sqliteErr.Message)
}

func TestConn_Filename(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	conn, cleanup := newConnVolatile(t)
	defer cleanup()

	assert.Equal(t, "test.db", conn.Filename())
}

func TestConn_Exec_Error(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	conn, cleanup := newConnUnix(t)
	defer cleanup()

	err := conn.Exec("INVALID sql")
	assert.EqualError(t, err, "near \"INVALID\": syntax error")
}

func TestConn_Exec(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	conn, cleanup := newConnUnix(t)
	defer cleanup()

	err := conn.Exec("CREATE TABLE foo (n INT)")
	assert.NoError(t, err)
}

func TestConn_Query_Error(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	conn, cleanup := newConnUnix(t)
	defer cleanup()

	_, err := conn.Query("SELECT * FROM foo")
	assert.EqualError(t, err, "no such table: foo")
}

func TestConn_Query(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	conn, cleanup := newConnUnix(t)
	defer cleanup()

	err := conn.Exec("CREATE TABLE foo (n INT)")
	require.NoError(t, err)

	err = conn.Exec("INSERT INTO foo(n) VALUES(1)")
	require.NoError(t, err)

	rows, err := conn.Query("SELECT * FROM foo")
	require.NoError(t, err)

	values := make([]driver.Value, 1)

	err = rows.Next(values)
	require.NoError(t, err)

	err = rows.Next(values)
	require.Equal(t, io.EOF, err)

	err = rows.Close()
	require.NoError(t, err)

	assert.Equal(t, int64(1), values[0])
}

func TestConn_CloseError(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	vfs, cleanup := newVfs(t)
	defer cleanup()

	conn, err := bindings.Open("test.db", vfs.Name())
	assert.NoError(t, err)

	err = conn.Exec("PRAGMA synchronous=OFF")
	require.NoError(t, err)

	err = conn.Exec("CREATE TABLE foo (n INT)")
	require.NoError(t, err)

	rows, err := conn.Query("SELECT * FROM foo")
	require.NoError(t, err)

	err = conn.Close()
	assert.EqualError(t, err, "unable to close due to unfinalized statements or unfinished backups")

	err = rows.Close()
	require.NoError(t, err)

	err = conn.Close()
	require.NoError(t, err)
}

// Create a new connection using dqlite's volatile VFS and setting WAL mode.
func newConnVolatileWal(t *testing.T) (*bindings.Conn, func()) {
	conn, cleanup := newConnVolatile(t)

	err := conn.Exec("PRAGMA synchronous=OFF")
	require.NoError(t, err)

	err = conn.Exec("PRAGMA journal_mode=wal")
	require.NoError(t, err)

	flag, err := conn.ConfigNoCkptOnClose(true)
	require.NoError(t, err)

	assert.True(t, flag)

	return conn, cleanup
}

// Create a new connection using dqlite's volatile VFS.
func newConnVolatile(t *testing.T) (*bindings.Conn, func()) {
	t.Helper()

	vfs, vfsCleanup := newVfs(t)

	conn, connCleanup := newConnWithVfs(t, "test.db", vfs.Name())

	cleanup := func() {
		connCleanup()
		vfsCleanup()
	}

	return conn, cleanup
}

// Create a new connection using the unix VFS.
func newConnUnix(t *testing.T) (*bindings.Conn, func()) {
	t.Helper()

	dir, err := ioutil.TempDir("", "go-dqlite-bindings-")
	require.NoError(t, err)

	name := filepath.Join(dir, "test.db")
	conn, connCleanup := newConnWithVfs(t, name, "unix")

	cleanup := func() {
		connCleanup()
		require.NoError(t, os.RemoveAll(dir))
	}

	return conn, cleanup
}

func newConnWithVfs(t *testing.T, name string, vfs string) (*bindings.Conn, func()) {
	t.Helper()

	conn, err := bindings.Open(name, vfs)
	require.NoError(t, err)

	cleanup := func() {
		require.NoError(t, conn.Close())
	}

	return conn, cleanup
}
