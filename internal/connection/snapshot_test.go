package connection_test

import (
	"database/sql/driver"
	"io"
	"testing"

	"github.com/CanonicalLtd/go-dqlite/internal/bindings"
	"github.com/CanonicalLtd/go-dqlite/internal/connection"
	"github.com/CanonicalLtd/go-dqlite/internal/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshot(t *testing.T) {
	logger := bindings.NewLogger(logging.Test(t))
	defer logger.Close()

	vfs, err := bindings.NewVfs("test", logger)
	require.NoError(t, err)

	defer vfs.Close()

	// Create a database with some content.
	conn, err := bindings.Open("test.db", "test")
	require.NoError(t, err)

	err = conn.Exec("PRAGMA synchronous=OFF; PRAGMA journal_mode=wal")
	require.NoError(t, err)

	err = conn.Exec("CREATE TABLE foo (n INT); INSERT INTO foo VALUES(1)")
	require.NoError(t, err)

	// Perform the snapshot.
	database, wal, err := connection.Snapshot(vfs, "test.db")
	require.NoError(t, err)

	require.NoError(t, conn.Close())

	// Restore the snapshot.
	require.NoError(t, connection.Restore(vfs, "test.db", database, wal))

	// Check that the data actually matches our source database.
	conn, err = bindings.Open("test.db", "test")
	require.NoError(t, err)
	defer conn.Close()

	rows, err := conn.Query("SELECT * FROM foo")
	require.NoError(t, err)

	values := make([]driver.Value, 1)
	assert.Equal(t, nil, rows.Next(values))
	assert.Equal(t, int64(1), values[0])
	assert.Equal(t, io.EOF, rows.Next(values))
}
