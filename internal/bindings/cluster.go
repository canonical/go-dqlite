package bindings

// ServerInfo is the Go equivalent of dqlite_server.
type ServerInfo struct {
	ID      uint64
	Address string
}
