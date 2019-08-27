package dqlite

import (
	"context"
	"fmt"
	"time"

	"github.com/Rican7/retry/backoff"
	"github.com/Rican7/retry/strategy"
	"github.com/canonical/go-dqlite/internal/bindings"
	"github.com/canonical/go-dqlite/internal/protocol"
	"github.com/pkg/errors"
)

// ServerInfo holds information about a single server.
type ServerInfo = protocol.ServerInfo

// Server implements the dqlite network protocol.
type Server struct {
	log         LogFunc          // Logger
	server      *bindings.Server // Low-level C implementation
	acceptCh    chan error       // Receives connection handling errors
	id          uint64
	address     string
	bindAddress string
}

// ServerOption can be used to tweak server parameters.
type ServerOption func(*serverOptions)

// WithServerLogFunc sets a custom log function for the server.
func WithServerLogFunc(log LogFunc) ServerOption {
	return func(options *serverOptions) {
		options.Log = log
	}
}

// WithServerDialFunc sets a custom dial function for the server.
func WithServerDialFunc(dial DialFunc) ServerOption {
	return func(options *serverOptions) {
		options.DialFunc = dial
	}
}

// WithBindAddress sets a custom bind address for the server.
func WithServerBindAddress(address string) ServerOption {
	return func(options *serverOptions) {
		options.BindAddress = address
	}
}

// NewServer creates a new Server instance.
func NewServer(info ServerInfo, dir string, options ...ServerOption) (*Server, error) {
	o := defaultServerOptions()

	for _, option := range options {
		option(o)
	}

	server, err := bindings.NewServer(uint(info.ID), info.Address, dir)
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
	s := &Server{
		log:         o.Log,
		server:      server,
		acceptCh:    make(chan error, 1),
		id:          info.ID,
		address:     info.Address,
		bindAddress: bindAddress,
	}

	return s, nil
}

// Cluster returns information about all servers in the cluster.
func (s *Server) Cluster(ctx context.Context) ([]ServerInfo, error) {
	c, err := protocol.Connect(ctx, protocol.UnixDial, s.bindAddress, protocol.VersionLegacy)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to dqlite task")
	}
	defer c.Close()

	request := protocol.Message{}
	request.Init(16)
	response := protocol.Message{}
	response.Init(512)

	protocol.EncodeCluster(&request)

	if err := c.Call(ctx, &request, &response); err != nil {
		return nil, errors.Wrap(err, "failed to send Cluster request")
	}

	servers, err := protocol.DecodeServers(&response)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse Server response")
	}

	return servers, nil
}

// Leader returns information about the current leader, if any.
func (s *Server) LeaderAddress(ctx context.Context) (string, error) {
	p, err := protocol.Connect(ctx, protocol.UnixDial, s.bindAddress, protocol.VersionLegacy)
	if err != nil {
		return "", errors.Wrap(err, "failed to connect to dqlite task")
	}
	defer p.Close()

	request := protocol.Message{}
	request.Init(16)
	response := protocol.Message{}
	response.Init(512)

	protocol.EncodeLeader(&request)

	if err := p.Call(ctx, &request, &response); err != nil {
		return "", errors.Wrap(err, "failed to send Leader request")
	}

	_, leader, err := protocol.DecodeServerCompat(p, &response)
	if err != nil {
		return "", errors.Wrap(err, "failed to parse Server response")
	}

	return leader, nil
}

// Start serving requests.
func (s *Server) Start() error {
	return s.server.Start()
}

// Join a cluster.
func (s *Server) Join(ctx context.Context, store ServerStore, dial DialFunc) error {
	if dial == nil {
		dial = protocol.TCPDial
	}
	config := protocol.Config{
		Dial:           protocol.DialFunc(dial),
		AttemptTimeout: time.Second,
		RetryStrategies: []strategy.Strategy{
			strategy.Backoff(backoff.BinaryExponential(time.Millisecond))},
	}
	connector := protocol.NewConnector(0, store, config, s.log)
	c, err := connector.Connect(ctx)
	if err != nil {
		return err
	}
	defer c.Close()

	request := protocol.Message{}
	request.Init(4096)
	response := protocol.Message{}
	response.Init(4096)

	protocol.EncodeJoin(&request, s.id, s.address)

	if err := c.Call(ctx, &request, &response); err != nil {
		return err
	}

	protocol.EncodePromote(&request, s.id)

	if err := c.Call(ctx, &request, &response); err != nil {
		return err
	}

	return nil
}

// Leave a cluster.
func Leave(ctx context.Context, id uint64, store ServerStore, dial DialFunc) error {
	if dial == nil {
		dial = protocol.TCPDial
	}
	config := protocol.Config{
		Dial:           protocol.DialFunc(dial),
		AttemptTimeout: time.Second,
		RetryStrategies: []strategy.Strategy{
			strategy.Backoff(backoff.BinaryExponential(time.Millisecond))},
	}
	connector := protocol.NewConnector(0, store, config, defaultLogFunc())
	c, err := connector.Connect(ctx)
	if err != nil {
		return err
	}
	defer c.Close()

	request := protocol.Message{}
	request.Init(4096)
	response := protocol.Message{}
	response.Init(4096)

	protocol.EncodeRemove(&request, id)

	if err := c.Call(ctx, &request, &response); err != nil {
		return err
	}

	return nil
}

// Hold configuration options for a dqlite server.
type serverOptions struct {
	Log         LogFunc
	DialFunc    DialFunc
	BindAddress string
}

type File struct {
	Name string
	Data []byte
}

func (s *Server) Dump(ctx context.Context, filename string) ([]File, error) {
	c, err := protocol.Connect(ctx, protocol.UnixDial, s.bindAddress, protocol.VersionLegacy)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to dqlite task")
	}
	defer c.Close()

	request := protocol.Message{}
	request.Init(16)
	response := protocol.Message{}
	response.Init(512)

	protocol.EncodeDump(&request, filename)

	if err := c.Call(ctx, &request, &response); err != nil {
		return nil, errors.Wrap(err, "failed to send dump request")
	}

	files, err := protocol.DecodeFiles(&response)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse files response")
	}
	defer files.Close()

	dump := make([]File, 0)

	for {
		name, data := files.Next()
		if name == "" {
			break
		}
		dump = append(dump, File{Name: name, Data: data})
	}

	return dump, nil
}

// Close the server, releasing all resources it created.
func (s *Server) Close() error {
	// Send a stop signal to the dqlite event loop.
	if err := s.server.Stop(); err != nil {
		return errors.Wrap(err, "server failed to stop")
	}

	s.server.Close()

	return nil
}

// Create a serverOptions object with sane defaults.
func defaultServerOptions() *serverOptions {
	return &serverOptions{
		Log: defaultLogFunc(),
	}
}
