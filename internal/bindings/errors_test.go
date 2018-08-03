package bindings_test

import (
	"fmt"
	"testing"

	"github.com/CanonicalLtd/go-dqlite/internal/bindings"
	"github.com/stretchr/testify/assert"
)

// Extract the error code from a SQLite error.
func TestErrorCode_SQLiteError(t *testing.T) {
	err := bindings.Error{Code: 123}
	assert.Equal(t, 123, bindings.ErrorCode(err))
}

// Extract the error code from a generic error.
func TestErrorCode_GenericError(t *testing.T) {
	err := fmt.Errorf("boom")
	assert.Equal(t, bindings.ErrError, bindings.ErrorCode(err))
}
