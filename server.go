package dqlite

import (
	"fmt"

	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/go-dqlite/internal/bindings"
	"github.com/canonical/go-dqlite/internal/protocol"
	"github.com/pkg/errors"
)

// Node runs a dqlite node.
type Node struct {
	log         client.LogFunc // Logger
	server      *bindings.Node // Low-level C implementation
	acceptCh    chan error     // Receives connection handling errors
	id          uint64
	address     string
	bindAddress string
}

// NodeOption can be used to tweak node parameters.
type NodeOption func(*serverOptions)

// WithNodeLogFunc sets a custom log function for the server.
func WithNodeLogFunc(log client.LogFunc) NodeOption {
	return func(options *serverOptions) {
		options.Log = log
	}
}

// WithNodeDialFunc sets a custom dial function for the server.
func WithNodeDialFunc(dial client.DialFunc) NodeOption {
	return func(options *serverOptions) {
		options.DialFunc = dial
	}
}

// WithBindAddress sets a custom bind address for the server.
func WithNodeBindAddress(address string) NodeOption {
	return func(options *serverOptions) {
		options.BindAddress = address
	}
}

// NewNode creates a new Node instance.
func NewNode(info client.NodeInfo, dir string, options ...NodeOption) (*Node, error) {
	o := defaultNodeOptions()

	for _, option := range options {
		option(o)
	}

	server, err := bindings.NewNode(uint(info.ID), info.Address, dir)
	if err != nil {
		return nil, err
	}
	if o.DialFunc != nil {
		if err := server.SetDialFunc(protocol.DialFunc(o.DialFunc)); err != nil {
			return nil, err
		}
	}
	bindAddress := fmt.Sprintf("@dqlite-%d", info.ID)
	if o.BindAddress != "" {
		bindAddress = o.BindAddress
	}
	if err := server.SetBindAddress(bindAddress); err != nil {
		return nil, err
	}
	s := &Node{
		log:         o.Log,
		server:      server,
		acceptCh:    make(chan error, 1),
		id:          info.ID,
		address:     info.Address,
		bindAddress: bindAddress,
	}

	return s, nil
}

// BindAddress returns the network address the node is listening to.
func (s *Node) BindAddress() string {
	return s.server.GetBindAddress()
}

// Start serving requests.
func (s *Node) Start() error {
	return s.server.Start()
}

// Hold configuration options for a dqlite server.
type serverOptions struct {
	Log         client.LogFunc
	DialFunc    client.DialFunc
	BindAddress string
}

// Close the server, releasing all resources it created.
func (s *Node) Close() error {
	// Send a stop signal to the dqlite event loop.
	if err := s.server.Stop(); err != nil {
		return errors.Wrap(err, "server failed to stop")
	}

	s.server.Close()

	return nil
}

// Create a serverOptions object with sane defaults.
func defaultNodeOptions() *serverOptions {
	return &serverOptions{
		Log:      client.DefaultLogFunc,
		DialFunc: client.DefaultDialFunc,
	}
}
