package bindings_test

import (
	"testing"

	"github.com/CanonicalLtd/go-dqlite/internal/bindings"
	"github.com/CanonicalLtd/go-dqlite/internal/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStatusMallocCount(t *testing.T) {
	_, _, err := bindings.StatusMallocCount(true)
	require.NoError(t, err)

	current, highest, err := bindings.StatusMallocCount(false)
	require.NoError(t, err)

	assert.Equal(t, 0, current)
	assert.Equal(t, 0, highest)

	logger := bindings.NewLogger(logging.Test(t))

	// Create a volatile VFS to perform some allocations.
	vfs, err := bindings.NewVfs("test", logger)
	require.NoError(t, err)

	current, highest, err = bindings.StatusMallocCount(false)
	require.NoError(t, err)

	assert.NotEqual(t, 0, current)
	assert.NotEqual(t, 0, highest)

	vfs.Close()
	logger.Close()

	current, highest, err = bindings.StatusMallocCount(true)
	require.NoError(t, err)

	assert.Equal(t, 0, current)
	assert.NotEqual(t, 0, highest)

	current, highest, err = bindings.StatusMallocCount(false)
	require.NoError(t, err)

	assert.Equal(t, 0, current)
	assert.Equal(t, 0, highest)
}

func TestStatusMemoryUsed(t *testing.T) {
	_, _, err := bindings.StatusMemoryUsed(true)
	require.NoError(t, err)

	current, highest, err := bindings.StatusMemoryUsed(false)
	require.NoError(t, err)

	assert.Equal(t, 0, current)
	assert.Equal(t, 0, highest)

	logger := bindings.NewLogger(logging.Test(t))

	// Create a volatile VFS to perform some allocations.
	vfs, err := bindings.NewVfs("test", logger)
	require.NoError(t, err)

	current, highest, err = bindings.StatusMemoryUsed(false)
	require.NoError(t, err)

	assert.NotEqual(t, 0, current)
	assert.NotEqual(t, 0, highest)

	vfs.Close()
	logger.Close()

	current, highest, err = bindings.StatusMemoryUsed(true)
	require.NoError(t, err)

	assert.Equal(t, 0, current)
	assert.NotEqual(t, 0, highest)

	current, highest, err = bindings.StatusMemoryUsed(false)
	require.NoError(t, err)

	assert.Equal(t, 0, current)
	assert.Equal(t, 0, highest)
}
