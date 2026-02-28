package driver_test

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"

	dqlitedriver "github.com/canonical/go-dqlite/v3/driver"
	"github.com/canonical/go-dqlite/v3/internal/protocol"
	"github.com/stretchr/testify/assert"
)

func TestIsRetriableError_Nil(t *testing.T) {
	assert.False(t, dqlitedriver.IsRetriableError(nil))
}

// Test driver.ErrBadConn - the standard Go signal for bad connections
func TestIsRetriableError_ErrBadConn(t *testing.T) {
	assert.True(t, dqlitedriver.IsRetriableError(driver.ErrBadConn))
}

func TestIsRetriableError_ErrBadConn_Wrapped(t *testing.T) {
	err := fmt.Errorf("connection failed: %w", driver.ErrBadConn)
	assert.True(t, dqlitedriver.IsRetriableError(err))
}

// Test ErrNoAvailableLeader - implements Retryable, always returns true
func TestIsRetriableError_ErrNoAvailableLeader(t *testing.T) {
	assert.True(t, dqlitedriver.IsRetriableError(dqlitedriver.ErrNoAvailableLeader))
}

func TestIsRetriableError_ErrNoAvailableLeader_Wrapped(t *testing.T) {
	err := fmt.Errorf("leader lookup failed: %w", dqlitedriver.ErrNoAvailableLeader)
	assert.True(t, dqlitedriver.IsRetriableError(err))
}

// Test protocol.Error (SQLite errors) - implements Retryable
func TestIsRetriableError_ErrBusy(t *testing.T) {
	err := dqlitedriver.Error{Code: dqlitedriver.ErrBusy, Message: "database is busy"}
	assert.True(t, dqlitedriver.IsRetriableError(err))
}

func TestIsRetriableError_ErrBusyRecovery(t *testing.T) {
	err := dqlitedriver.Error{Code: dqlitedriver.ErrBusyRecovery, Message: "database is busy (recovery)"}
	assert.True(t, dqlitedriver.IsRetriableError(err))
}

func TestIsRetriableError_ErrBusySnapshot(t *testing.T) {
	err := dqlitedriver.Error{Code: dqlitedriver.ErrBusySnapshot, Message: "database is busy (snapshot)"}
	assert.True(t, dqlitedriver.IsRetriableError(err))
}

func TestIsRetriableError_ErrLocked(t *testing.T) {
	// SQLite SQLITE_LOCKED error code is 6
	err := dqlitedriver.Error{Code: 6, Message: "database is locked"}
	assert.True(t, dqlitedriver.IsRetriableError(err))
}

func TestIsRetriableError_WrappedBusy(t *testing.T) {
	inner := dqlitedriver.Error{Code: dqlitedriver.ErrBusy, Message: "database is busy"}
	err := fmt.Errorf("exec failed: %w", inner)
	assert.True(t, dqlitedriver.IsRetriableError(err))
}

// Test protocol.ErrRequest - implements Retryable
func TestIsRetriableError_ErrRequest_NotLeader(t *testing.T) {
	err := protocol.ErrRequest{Code: uint64(dqlitedriver.ErrIoErrNotLeader), Description: "not leader"}
	assert.True(t, dqlitedriver.IsRetriableError(err))
}

func TestIsRetriableError_ErrRequest_LeadershipLost(t *testing.T) {
	err := protocol.ErrRequest{Code: uint64(dqlitedriver.ErrIoErrLeadershipLost), Description: "leadership lost"}
	assert.True(t, dqlitedriver.IsRetriableError(err))
}

func TestIsRetriableError_ErrRequest_Wrapped(t *testing.T) {
	inner := protocol.ErrRequest{Code: uint64(dqlitedriver.ErrIoErrNotLeader), Description: "not leader"}
	err := fmt.Errorf("operation failed: %w", inner)
	assert.True(t, dqlitedriver.IsRetriableError(err))
}

// Test non-retryable errors
func TestIsRetriableError_NonRetryable_PlainError(t *testing.T) {
	err := errors.New("UNIQUE constraint failed: test.name")
	assert.False(t, dqlitedriver.IsRetriableError(err))
}

func TestIsRetriableError_NonRetryable_ConstraintError(t *testing.T) {
	// SQLite SQLITE_CONSTRAINT error code is 19
	err := dqlitedriver.Error{Code: 19, Message: "UNIQUE constraint failed"}
	assert.False(t, dqlitedriver.IsRetriableError(err))
}

func TestIsRetriableError_NonRetryable_ErrRequest(t *testing.T) {
	// Some other protocol error that's not leadership-related
	err := protocol.ErrRequest{Code: 1, Description: "some other error"}
	assert.False(t, dqlitedriver.IsRetriableError(err))
}

func TestIsRetriableError_NonRetryable_StringError(t *testing.T) {
	// String errors should NOT be retryable - we only check typed errors
	err := errors.New("database is locked")
	assert.False(t, dqlitedriver.IsRetriableError(err))
}

func TestIsRetriableError_NonRetryable_BadConnectionString(t *testing.T) {
	// String "bad connection" should NOT match - we check driver.ErrBadConn
	err := errors.New("bad connection")
	assert.False(t, dqlitedriver.IsRetriableError(err))
}

// Test that the Retryable interface works correctly
func TestRetryable_Interface(t *testing.T) {
	// protocol.Error implements Retryable
	var _ protocol.Retryable = dqlitedriver.Error{}

	// protocol.ErrRequest implements Retryable
	var _ protocol.Retryable = protocol.ErrRequest{}

	// ErrNoAvailableLeader implements Retryable
	var retryable protocol.Retryable
	assert.True(t, errors.As(dqlitedriver.ErrNoAvailableLeader, &retryable))
}
