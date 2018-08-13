package bindings_test

import (
	"testing"

	"github.com/CanonicalLtd/go-dqlite/internal/bindings"
	"github.com/CanonicalLtd/go-dqlite/internal/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewVfs_AlreadyRegistered(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	vfs, cleanup := newVfsWithName(t, "foo")
	defer cleanup()

	logger := bindings.NewLogger(logging.Test(t))
	defer logger.Close()

	vfs, err := bindings.NewVfs("foo", logger)
	assert.Nil(t, vfs)

	assert.EqualError(t, err, "vfs name already registered")
}

func TestNewVfs_ReadFile_Error(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	vfs, cleanup := newVfs(t)
	defer cleanup()

	data, err := vfs.ReadFile("test.db")

	assert.Nil(t, data)
	assert.EqualError(t, err, "unable to open database file")
}

func TestNewVfs_ReadFile_Empty(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	vfs, cleanup := newVfs(t)
	defer cleanup()

	_, cleanup = newConnWithVfs(t, "test.db", "test")
	defer cleanup()

	data, err := vfs.ReadFile("test.db")
	require.NoError(t, err)

	assert.Len(t, data, 0)
}

func TestNewVfs_ReadFile(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	vfs, cleanup := newVfs(t)
	defer cleanup()

	conn, cleanup := newConnWithVfs(t, "test.db", "test")
	defer cleanup()

	err := conn.Exec("PRAGMA synchronous=OFF")
	require.NoError(t, err)

	err = conn.Exec("CREATE TABLE test (n INT)")
	require.NoError(t, err)

	data, err := vfs.ReadFile("test.db")
	require.NoError(t, err)

	assert.Len(t, data, 8192)
}

func TestNewVfs_WriteFile(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	vfs1, cleanup := newVfsWithName(t, "test1")
	defer cleanup()

	conn, cleanup := newConnWithVfs(t, "test.db", "test1")
	defer cleanup()

	err := conn.Exec("PRAGMA synchronous=OFF")
	require.NoError(t, err)

	err = conn.Exec("CREATE TABLE test (n INT)")
	require.NoError(t, err)

	data1, err := vfs1.ReadFile("test.db")
	require.NoError(t, err)

	assert.Len(t, data1, 8192)

	vfs2, cleanup := newVfsWithName(t, "test2")
	defer cleanup()

	err = vfs2.WriteFile("test.db", data1)
	require.NoError(t, err)

	data2, err := vfs2.ReadFile("test.db")
	require.NoError(t, err)

	assert.Equal(t, data1, data2)
}

func newVfs(t *testing.T) (*bindings.Vfs, func()) {
	t.Helper()
	return newVfsWithName(t, "test")
}

func newVfsWithName(t *testing.T, name string) (*bindings.Vfs, func()) {
	t.Helper()

	logger := bindings.NewLogger(logging.Test(t))

	vfs, err := bindings.NewVfs(name, logger)
	require.NoError(t, err)

	cleanup := func() {
		require.NoError(t, vfs.Close())
		logger.Close()
	}

	return vfs, cleanup
}
