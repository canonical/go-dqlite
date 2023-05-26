package client

// TxMode defines the transaction mode to use when opening a new transaction.
type TxMode = int

const (
	// TxModeBegin is the default transaction mode.
	TxModeBegin TxMode = iota

	// TxModeBeginConcurrent is a custom transaction mode that allows multiple
	// writers to process write transactions simultaneously.
	// https://www.sqlite.org/cgi/src/doc/begin-concurrent/doc/begin_concurrent.md#:~:text=Overview,system%20still%20serializes%20COMMIT%20commands.
	TxModeBeginConcurrent
)
