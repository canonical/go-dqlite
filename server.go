package dqlite

import (
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"time"

	"github.com/Rican7/retry/backoff"
	"github.com/Rican7/retry/strategy"
	"github.com/canonical/go-dqlite/internal/bindings"
	"github.com/canonical/go-dqlite/internal/client"
	"github.com/canonical/go-dqlite/internal/logging"
	"github.com/pkg/errors"
)

// ServerInfo holds information about a single server.
type ServerInfo = client.ServerInfo

// WatchFunc notifies about state changes.
type WatchFunc = bindings.WatchFunc

// States
const (
	Unavailable = bindings.Unavailable
	Follower    = bindings.Follower
	Candidate   = bindings.Candidate
	Leader      = bindings.Leader
)

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

// WithServerWatchFunc sets a function that will be invoked
// whenever this server acquires leadership.
func WithServerWatchFunc(watch WatchFunc) ServerOption {
	return func(options *serverOptions) {
		options.WatchFunc = watch
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
		if err := server.SetDialFunc(bindings.DialFunc(o.DialFunc)); err != nil {
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
	log := func(level int, msg string) {
	}
	if o.Log != nil {
		log = func(level int, msg string) {
			o.Log(logging.Level(level), msg)
		}
	}
	server.SetLogFunc(log)
	if o.WatchFunc != nil {
		server.SetWatchFunc(o.WatchFunc)
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
func (s *Server) Cluster() ([]ServerInfo, error) {
	return s.server.Cluster()
}

// Leader returns information about the current leader, if any.
func (s *Server) LeaderAddress(ctx context.Context) (string, error) {
	store := NewInmemServerStore()
	c, err := client.Connect(ctx, client.UnixDial, s.bindAddress, store, s.log)
	if err != nil {
		return "", errors.Wrap(err, "failed to connect to dqlite task")
	}
	defer c.Close()

	request := client.Message{}
	request.Init(16)
	response := client.Message{}
	response.Init(512)

	client.EncodeLeader(&request)

	if err := c.Call(ctx, &request, &response); err != nil {
		return "", errors.Wrap(err, "failed to send Leader request")
	}

	leader, err := client.DecodeServer(&response)
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
		dial = client.TCPDial
	}
	config := client.Config{
		Dial:           bindings.DialFunc(dial),
		AttemptTimeout: time.Second,
		RetryStrategies: []strategy.Strategy{
			strategy.Backoff(backoff.BinaryExponential(time.Millisecond))},
	}
	connector := client.NewConnector(0, store, config, defaultLogFunc())
	c, err := connector.Connect(ctx)
	if err != nil {
		return err
	}
	defer c.Close()

	request := client.Message{}
	request.Init(4096)
	response := client.Message{}
	response.Init(4096)

	client.EncodeJoin(&request, s.id, s.address)

	if err := c.Call(ctx, &request, &response); err != nil {
		return err
	}

	client.EncodePromote(&request, s.id)

	if err := c.Call(ctx, &request, &response); err != nil {
		return err
	}

	return nil
}

// Leave a cluster.
func Leave(ctx context.Context, id uint64, store ServerStore, dial DialFunc) error {
	if dial == nil {
		dial = client.TCPDial
	}
	config := client.Config{
		Dial:           bindings.DialFunc(dial),
		AttemptTimeout: time.Second,
		RetryStrategies: []strategy.Strategy{
			strategy.Backoff(backoff.BinaryExponential(time.Millisecond))},
	}
	connector := client.NewConnector(0, store, config, defaultLogFunc())
	c, err := connector.Connect(ctx)
	if err != nil {
		return err
	}
	defer c.Close()

	request := client.Message{}
	request.Init(4096)
	response := client.Message{}
	response.Init(4096)

	client.EncodeRemove(&request, id)

	if err := c.Call(ctx, &request, &response); err != nil {
		return err
	}

	return nil
}

// Hold configuration options for a dqlite server.
type serverOptions struct {
	Log         LogFunc
	DialFunc    DialFunc
	WatchFunc   WatchFunc
	BindAddress string
}

// Dump the files of a database to disk.
func (s *Server) Dump(name string, dir string) error {
	// Dump the database file.
	bytes, err := s.server.Dump(name)
	if err != nil {
		return errors.Wrap(err, "failed to get database file content")
	}

	path := filepath.Join(dir, name)
	if err := ioutil.WriteFile(path, bytes, 0600); err != nil {
		return errors.Wrap(err, "failed to write database file")
	}

	// Dump the WAL file.
	bytes, err = s.server.Dump(name + "-wal")
	if err != nil {
		return errors.Wrap(err, "failed to get WAL file content")
	}

	path = filepath.Join(dir, name+"-wal")
	if err := ioutil.WriteFile(path, bytes, 0600); err != nil {
		return errors.Wrap(err, "failed to write WAL file")
	}

	return nil
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
