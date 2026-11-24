package dqlite

// ConfigMultiThread sets the threading mode of SQLite to Multi-thread.
//
// DEPRECATED: go-dqlite does not set single-thread mode anymore and this API
// is now just a harmless no-op.
func ConfigMultiThread() error {
	return nil
}
