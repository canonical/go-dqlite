package driver

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
)

// ErrIsolationLevelNotSupported is returned when an unsupported isolation
// level is requested in BeginTx.
type ErrIsolationLevelNotSupported struct {
	Level sql.IsolationLevel
}

func (e ErrIsolationLevelNotSupported) Error() string {
	return fmt.Sprintf("isolation level %q is not supported", e.Level)
}

// ErrReadOnlyNotSupported is returned when a read-only transaction is
// requested in BeginTx, which dqlite does not support.
type ErrReadOnlyNotSupported struct{}

func (e ErrReadOnlyNotSupported) Error() string {
	return "read-only transactions are not supported"
}

// validateTxOptions checks if the transaction options are supported by dqlite.
//
// dqlite (like SQLite) only supports the SERIALIZABLE isolation level
// (the default), and does not support read-only transactions.
func validateTxOptions(opts driver.TxOptions) error {
	return ValidateTxOptions(opts)
}

// ValidateTxOptions checks if the transaction options are supported by dqlite.
//
// dqlite (like SQLite) only supports the SERIALIZABLE isolation level
// (the default), and does not support read-only transactions.
//
// This function is exported for testing purposes.
func ValidateTxOptions(opts driver.TxOptions) error {
	// Check isolation level. SQLite/dqlite only supports SERIALIZABLE
	// (which is the default, represented by sql.LevelDefault or
	// sql.LevelSerializable).
	isolation := sql.IsolationLevel(opts.Isolation)
	switch isolation {
	case sql.LevelDefault, sql.LevelSerializable:
		// Supported
	default:
		return ErrIsolationLevelNotSupported{Level: isolation}
	}

	// Check read-only mode. dqlite does not support read-only transactions.
	if opts.ReadOnly {
		return ErrReadOnlyNotSupported{}
	}

	return nil
}
