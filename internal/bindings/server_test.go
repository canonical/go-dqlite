package bindings_test

import (
	"encoding/binary"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"github.com/canonical/go-dqlite/internal/bindings"
	"github.com/canonical/go-dqlite/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServer_Create(t *testing.T) {
	_, cleanup := newServer(t)
	defer cleanup()
}

func TestServer_Leader(t *testing.T) {
	_, cleanup := newServer(t)
	defer cleanup()

	conn := newClient(t)

	// Make a Leader request
	buf := makeClientRequest(t, conn, protocol.RequestLeader)
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

// func TestServer_ConcurrentHandleAndClose(t *testing.T) {
// 	server, cleanup := newServer(t)
// 	defer cleanup()

// 	listener, cleanup := newListener(t)
// 	defer cleanup()

// 	acceptCh := make(chan error)
// 	go func() {
// 		conn, err := listener.Accept()
// 		if err != nil {
// 			acceptCh <- err
// 		}
// 		server.Handle(conn)
// 		acceptCh <- nil
// 	}()

// 	conn, err := net.Dial("unix", listener.Addr().String())
// 	require.NoError(t, err)

// 	require.NoError(t, conn.Close())

// 	assert.NoError(t, <-acceptCh)
// }

// Create a new Server object for tests.
func newServer(t *testing.T) (*bindings.Server, func()) {
	t.Helper()

	dir, dirCleanup := newDir(t)

	server, err := bindings.NewServer(1, "1", dir)
	require.NoError(t, err)

	err = server.SetBindAddress("@test")
	require.NoError(t, err)

	require.NoError(t, server.Start())

	cleanup := func() {
		require.NoError(t, server.Stop())
		server.Close()
		dirCleanup()
	}

	return server, cleanup
}

// Create a new client network connection, performing the handshake.
func newClient(t *testing.T) net.Conn {
	t.Helper()

	conn, err := net.Dial("unix", "@test")
	require.NoError(t, err)

	// Handshake
	err = binary.Write(conn, binary.LittleEndian, protocol.ProtocolVersion)
	require.NoError(t, err)

	return conn
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
