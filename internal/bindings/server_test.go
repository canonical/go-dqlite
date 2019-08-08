package bindings_test

import (
	"encoding/binary"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"github.com/canonical/go-dqlite/internal/bindings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func nullLog(level int, msg string) {
}

func TestNewServer(t *testing.T) {
	dir, cleanup := newDir(t)
	defer cleanup()

	server, err := bindings.NewServer(1, "1", dir)
	require.NoError(t, err)

	server.Close()
}

func TestServer_Run(t *testing.T) {
	server, cleanup := newServer(t)
	defer cleanup()

	server.SetLogFunc(nullLog)

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
	server, cleanup := newServer(t)
	defer cleanup()

	listener, cleanup := newListener(t)
	defer cleanup()

	cleanup = runServer(t, server, listener)
	defer cleanup()

	conn := newClient(t, listener)

	// Make a Leader request
	buf := makeClientRequest(t, conn, bindings.RequestLeader)
	assert.Equal(t, uint8(1), buf[0])

	require.NoError(t, conn.Close())
}

// func TestServer_Heartbeat(t *testing.T) {
// 	server, cleanup := newServer(t)
// 	defer cleanup()

// 	listener, cleanup := newListener(t)
// 	defer cleanup()

// 	cleanup = runServer(t, server, listener)
// 	defer cleanup()

// 	conn := newClient(t, listener)

// 	// Make a Heartbeat request
// 	makeClientRequest(t, conn, bindings.RequestHeartbeat)

// 	require.NoError(t, conn.Close())
// }

func TestServer_ConcurrentHandleAndClose(t *testing.T) {
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

	dir, dirCleanup := newDir(t)

	server, err := bindings.NewServer(1, "1", dir)
	require.NoError(t, err)

	cleanup := func() {
		server.Close()
		dirCleanup()
	}

	return server, cleanup
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

// Return a new temporary directory.
func newDir(t *testing.T) (string, func()) {
	t.Helper()

	dir, err := ioutil.TempDir("", "dqlite-replication-test-")
	assert.NoError(t, err)

	cleanup := func() {
		_, err := os.Stat(dir)
		if err != nil {
			assert.True(t, os.IsNotExist(err))
		} else {
			assert.NoError(t, os.RemoveAll(dir))
		}
	}

	return dir, cleanup
}
