package bindings_test

import (
	"testing"

	"github.com/CanonicalLtd/go-dqlite/internal/bindings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConn_WalCheckpoint(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	conn, cleanup := newConnVolatileWal(t)
	defer cleanup()

	err := conn.Exec("CREATE TABLE foo (n INT)")
	require.NoError(t, err)

	err = conn.Exec("INSERT INTO foo(n) VALUES(1)")
	require.NoError(t, err)

	size, ckpt, err := conn.WalCheckpoint("main", bindings.WalCheckpointTruncate)
	require.NoError(t, err)

	assert.Equal(t, 0, size)
	assert.Equal(t, 0, ckpt)
}
