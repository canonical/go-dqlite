package protocol

import (
	"fmt"
)

// Client errors.
var (
	ErrNoAvailableLeader = fmt.Errorf("no available dqlite leader server found")
	errNegativeRead      = fmt.Errorf("reader returned negative count from Read")
)

// ErrRequest is returned in case of request failure.
type ErrRequest struct {
	Code        uint64
	Description string
}

func (e ErrRequest) Error() string {
	return fmt.Sprintf("%s (%d)", e.Description, e.Code)
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
