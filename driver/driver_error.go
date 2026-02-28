package driver

import (
	"database/sql/driver"
	"errors"

	"github.com/canonical/go-dqlite/v3/internal/protocol"
)

// IsRetriableError returns true if the given error is transient and the
// interaction can be safely retried.
//
// An error is considered retryable if:
//   - It implements the Retryable interface and returns true from IsRetryable()
//   - It is driver.ErrBadConn (Go's standard signal to retry with a new connection)
//
// Error types that implement Retryable include:
//   - protocol.Error (SQLite errors): retryable for SQLITE_BUSY and SQLITE_LOCKED
//   - protocol.ErrRequest: retryable for leadership-related errors
//   - protocol.ErrNoAvailableLeader: always retryable
//
// To make a new error type retryable, implement the Retryable interface:
//
//	type Retryable interface {
//	    IsRetryable() bool
//	}
func IsRetriableError(err error) bool {
	if err == nil {
		return false
	}

	// Check for driver.ErrBadConn - Go's standard signal that the connection
	// is bad and the operation should be retried with a new connection.
	// This is returned by driverError() for various transient conditions
	// including leadership loss, network errors, and EOF.
	if errors.Is(err, driver.ErrBadConn) {
		return true
	}

	// Check if any error in the chain implements Retryable.
	// This allows error types to self-identify as retryable.
	var retryable protocol.Retryable
	if errors.As(err, &retryable) {
		return retryable.IsRetryable()
	}

	return false
}
