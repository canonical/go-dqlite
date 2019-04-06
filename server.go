package dqlite

import (
	"fmt"
	"net"
	"runtime"
	"time"

	"github.com/CanonicalLtd/go-dqlite/internal/bindings"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

// Server implements the dqlite network protocol.
type Server struct {
	log      LogFunc          // Logger
	server   *bindings.Server // Low-level C implementation
	listener net.Listener     // Queue of new connections
	running  bool             // Whether the server is running
	runCh    chan error       // Receives the low-level C server return code
	acceptCh chan error       // Receives connection handling errors
}

// ServerOption can be used to tweak server parameters.
type ServerOption func(*serverOptions)

// WithServerLogFunc sets a custom log function for the server.
func WithServerLogFunc(log LogFunc) ServerOption {
	return func(options *serverOptions) {
		options.Log = log
	}
}

// WithServerAddressProvider sets a custom resolver for server addresses.
func WithServerAddressProvider(provider raft.ServerAddressProvider) ServerOption {
	return func(options *serverOptions) {
		options.AddressProvider = provider
	}
}

// NewServer creates a new Server instance.
func NewServer(id uint, dir string, listener net.Listener, options ...ServerOption) (*Server, error) {
	o := defaultServerOptions()

	address := listener.Addr().String()

	for _, option := range options {
		option(o)
	}

	server, err := bindings.NewServer(id, address, dir)
	if err != nil {
		return nil, err
	}

	s := &Server{
		log:      o.Log,
		server:   server,
		listener: listener,
		runCh:    make(chan error),
		acceptCh: make(chan error, 1),
	}

	return s, nil
}

// Bootstrap the server.
func (s *Server) Bootstrap(servers []ServerInfo) error {
	return s.server.Bootstrap(servers)
}

// Start serving requests.
func (s *Server) Start() error {
	go s.run()

	if !s.server.Ready() {
		return fmt.Errorf("server failed to start")
	}

	go s.acceptLoop()

	s.running = true

	return nil
}

// Hold configuration options for a dqlite server.
type serverOptions struct {
	Log             LogFunc
	AddressProvider raft.ServerAddressProvider
}

// Run the server.
func (s *Server) run() {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	s.runCh <- s.server.Run()
}

func (s *Server) acceptLoop() {
	s.log(LogDebug, "accepting connections")

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.acceptCh <- nil
			return
		}

		err = s.server.Handle(conn)
		if err != nil {
			if err == bindings.ErrServerStopped {
				// Ignore failures due to the server being
				// stopped.
				err = nil
			}
			s.acceptCh <- err
			return
		}
	}
}

// Dump the files of a database to disk.
// func (s *Server) Dump(name string, dir string) error {
// 	// Dump the database file.
// 	bytes, err := s.registry.vfs.ReadFile(name)
// 	if err != nil {
// 		return errors.Wrap(err, "failed to get database file content")
// 	}

// 	path := filepath.Join(dir, name)
// 	if err := ioutil.WriteFile(path, bytes, 0600); err != nil {
// 		return errors.Wrap(err, "failed to write database file")
// 	}

// 	// Dump the WAL file.
// 	bytes, err = s.registry.vfs.ReadFile(name + "-wal")
// 	if err != nil {
// 		return errors.Wrap(err, "failed to get WAL file content")
// 	}

// 	path = filepath.Join(dir, name+"-wal")
// 	if err := ioutil.WriteFile(path, bytes, 0600); err != nil {
// 		return errors.Wrap(err, "failed to write WAL file")
// 	}

// 	return nil
// }

// Close the server, releasing all resources it created.
func (s *Server) Close() error {
	// Close the listener, which will make the listener.Accept() call in
	// acceptLoop() return an error.
	if err := s.listener.Close(); err != nil {
		return err
	}

	if !s.running {
		goto out
	}

	// Wait for the acceptLoop goroutine to exit.
	select {
	case err := <-s.acceptCh:
		if err != nil {
			return errors.Wrap(err, "accept goroutine failed")
		}
	case <-time.After(time.Second):
		return fmt.Errorf("accept goroutine did not stop within a second")
	}

	// Send a stop signal to the dqlite event loop.
	if err := s.server.Stop(); err != nil {
		return errors.Wrap(err, "server failed to stop")
	}

	// Wait for the run goroutine to exit.
	select {
	case err := <-s.runCh:
		if err != nil {
			return errors.Wrap(err, "accept goroutine failed")
		}
	case <-time.After(time.Second):
		return fmt.Errorf("server did not stop within a second")
	}

out:
	s.server.Close()

	return nil
}

// Create a serverOptions object with sane defaults.
func defaultServerOptions() *serverOptions {
	return &serverOptions{
		Log:             defaultLogFunc(),
		AddressProvider: nil,
	}
}
