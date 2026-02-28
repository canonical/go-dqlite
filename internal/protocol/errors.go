package protocol

import (
	"fmt"
)

// Retryable is an interface for errors that can indicate whether they
// represent a transient condition that may succeed on retry.
type Retryable interface {
	IsRetryable() bool
}

// retryableError is a sentinel error that is always retryable.
type retryableError string

func (e retryableError) Error() string   { return string(e) }
func (e retryableError) IsRetryable() bool { return true }

// Client errors.
var (
	// ErrNoAvailableLeader is returned when no leader is available in the cluster.
	// This is a transient condition - retry may succeed once a leader is elected.
	ErrNoAvailableLeader error = retryableError("no available dqlite leader server found")
	errNegativeRead            = fmt.Errorf("reader returned negative count from Read")
)

// ErrRequest is returned in case of request failure.
type ErrRequest struct {
	Code        uint64
	Description string
}

func (e ErrRequest) Error() string {
	return fmt.Sprintf("%s (%d)", e.Description, e.Code)
}

// IsRetryable returns true if this request error represents a transient
// condition. Leadership-related errors are retryable because the cluster
// may elect a new leader or the client may connect to a different node.
func (e ErrRequest) IsRetryable() bool {
	// Error codes for leadership issues (these match the codes in driver/driver.go)
	const (
		errIoErr                     = 10
		errIoErrNotLeader            = errIoErr | (40 << 8)
		errIoErrLeadershipLost       = errIoErr | (41 << 8)
		errIoErrNotLeaderLegacy      = errIoErr | (32 << 8)
		errIoErrLeadershipLostLegacy = errIoErr | (33 << 8)
	)

	switch e.Code {
	case errIoErrNotLeader, errIoErrLeadershipLost,
		errIoErrNotLeaderLegacy, errIoErrLeadershipLostLegacy:
		return true
	}
	return false
}

// ErrRowsPart is returned when the first batch of a multi-response result
// batch is done.
var ErrRowsPart = fmt.Errorf("not all rows were returned in this response")

// Error holds information about a SQLite error.
type Error struct {
	Code    int
	Message string
}

func (e Error) Error() string {
	return e.Message
}

// IsRetryable returns true if this SQLite error represents a transient
// condition that may succeed on retry.
func (e Error) IsRetryable() bool {
	// SQLite error codes for transient conditions.
	const (
		sqliteBusy   = 5 // SQLITE_BUSY: database file is locked
		sqliteLocked = 6 // SQLITE_LOCKED: table in the database is locked
	)

	// Extract the primary error code (lower 8 bits).
	// SQLite extended error codes encode the primary code in the lower 8 bits.
	// For example: SQLITE_BUSY_RECOVERY = 5 | (1 << 8) = 261
	// The primary code is still 5 (SQLITE_BUSY).
	primaryCode := e.Code & 0xFF

	switch primaryCode {
	case sqliteBusy, sqliteLocked:
		return true
	}
	return false
}
