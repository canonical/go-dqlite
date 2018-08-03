package bindings_test

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/CanonicalLtd/go-dqlite/internal/bindings"
	"github.com/CanonicalLtd/go-dqlite/internal/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewServer(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	cluster, cleanup := newCluster(t)
	defer cleanup()

	server, err := bindings.NewServer(cluster)
	require.NoError(t, err)

	server.Close()
}

func TestServer_Run(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	server, cleanup := newServer(t)
	defer cleanup()

	ch := make(chan error)
	go func() {
		err := server.Run()
		ch <- err
	}()

	require.True(t, server.Ready())
	require.NoError(t, server.Stop())

	assert.NoError(t, <-ch)
}

func TestServer_Leader(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	server, cleanup := newServer(t)
	defer cleanup()

	listener, cleanup := newListener(t)
	defer cleanup()

	cleanup = runServer(t, server, listener)
	defer cleanup()

	conn := newClient(t, listener)

	// Make a Leader request
	buf := makeClientRequest(t, conn, bindings.RequestLeader)
	assert.Equal(t, uint8(2), buf[0])

	require.NoError(t, conn.Close())
}

func TestServer_Heartbeat(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	server, cleanup := newServer(t)
	defer cleanup()

	listener, cleanup := newListener(t)
	defer cleanup()

	cleanup = runServer(t, server, listener)
	defer cleanup()

	conn := newClient(t, listener)

	// Make a Heartbeat request
	makeClientRequest(t, conn, bindings.RequestHeartbeat)

	require.NoError(t, conn.Close())
}

func TestServer_ConcurrentHandleAndClose(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	server, cleanup := newServer(t)
	defer cleanup()

	listener, cleanup := newListener(t)
	defer cleanup()

	runCh := make(chan error)
	go func() {
		err := server.Run()
		runCh <- err
	}()

	require.True(t, server.Ready())

	acceptCh := make(chan error)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			acceptCh <- err
		}
		server.Handle(conn)
		acceptCh <- nil
	}()

	conn, err := net.Dial("unix", listener.Addr().String())
	require.NoError(t, err)

	require.NoError(t, conn.Close())

	require.NoError(t, server.Stop())

	assert.NoError(t, <-runCh)

	assert.NoError(t, <-acceptCh)
}

// Create a new Server object for tests.
func newServer(t *testing.T) (*bindings.Server, func()) {
	t.Helper()

	cluster, clusterCleanup := newCluster(t)

	server, err := bindings.NewServer(cluster)
	require.NoError(t, err)

	logger := bindings.NewLogger(logging.Test(t))

	server.SetLogger(logger)

	cleanup := func() {
		server.Close()
		logger.Close()
		clusterCleanup()
	}

	return server, cleanup
}

// Create a new Cluster object using the testClusterMethods.
func newCluster(t *testing.T) (*bindings.Cluster, func()) {
	t.Helper()

	methods := &testClusterMethods{}

	cluster, err := bindings.NewCluster(methods)
	require.NoError(t, err)

	cleanup := func() {
		cluster.Close()
	}

	return cluster, cleanup
}

// Create a new server unix socket.
func newListener(t *testing.T) (net.Listener, func()) {
	t.Helper()

	listener, err := net.Listen("unix", "")
	require.NoError(t, err)

	cleanup := func() {
		require.NoError(t, listener.Close())
	}

	return listener, cleanup
}

// Create a new client network connection, performing the handshake.
func newClient(t *testing.T, listener net.Listener) net.Conn {
	t.Helper()

	conn, err := net.Dial("unix", listener.Addr().String())
	require.NoError(t, err)

	// Handshake
	err = binary.Write(conn, binary.LittleEndian, bindings.ProtocolVersion)
	require.NoError(t, err)

	return conn
}

// Run the server handling connections from the given listener, and stop it in the
// cleanup function.
func runServer(t *testing.T, server *bindings.Server, listener net.Listener) func() {
	t.Helper()

	runCh := make(chan error)
	go func() {
		err := server.Run()
		runCh <- err
	}()

	require.True(t, server.Ready())

	acceptCh := make(chan error)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			acceptCh <- err
			return
		}
		err = server.Handle(conn)
		acceptCh <- err
	}()

	cleanup := func() {
		require.NoError(t, server.Stop())

		assert.NoError(t, <-runCh)
		assert.NoError(t, <-acceptCh)
	}

	return cleanup
}

// Perform a client request.
func makeClientRequest(t *testing.T, conn net.Conn, kind byte) []byte {
	t.Helper()

	// Number of words
	err := binary.Write(conn, binary.LittleEndian, uint32(1))
	require.NoError(t, err)

	// Type, flags, extra.
	n, err := conn.Write([]byte{kind, 0, 0, 0})
	require.NoError(t, err)
	require.Equal(t, 4, n)

	n, err = conn.Write([]byte{0, 0, 0, 0, 0, 0, 0, 0}) // Unused single-word request payload
	require.NoError(t, err)
	require.Equal(t, 8, n)

	// Read the response
	conn.SetDeadline(time.Now().Add(250 * time.Millisecond))
	buf := make([]byte, 64)
	_, err = conn.Read(buf)
	require.NoError(t, err)

	return buf
}
