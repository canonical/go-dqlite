package bindings_test

import (
	"encoding/binary"
	"net"
	"testing"

	"github.com/CanonicalLtd/go-dqlite/internal/bindings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServer_Lifecycle(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	cluster := newTestCluster()

	server, err := bindings.NewServer(cluster)
	require.NoError(t, err)

	server.Close()
}

func TestServer_Run(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	cluster := newTestCluster()

	server, err := bindings.NewServer(cluster)
	require.NoError(t, err)

	defer server.Close()

	ch := make(chan error)
	go func() {
		err := server.Run()
		ch <- err
	}()

	require.True(t, server.Ready())
	require.NoError(t, server.Stop())

	assert.NoError(t, <-ch)
}

func TestServer_Handle(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	cluster := newTestCluster()

	server, err := bindings.NewServer(cluster)
	require.NoError(t, err)

	defer server.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	ch := make(chan error)
	go func() {
		err := server.Run()
		ch <- err
	}()

	require.True(t, server.Ready())

	go func() {
		conn, err := listener.Accept()
		require.NoError(t, err)
		require.NoError(t, server.Handle(conn))
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)

	// Handshake
	err = binary.Write(conn, binary.LittleEndian, bindings.ProtocolVersion)
	require.NoError(t, err)

	// Make a Leader request
	err = binary.Write(conn, binary.LittleEndian, uint32(1)) // Words
	require.NoError(t, err)
	n, err := conn.Write([]byte{bindings.RequestLeader, 0, 0, 0}) // Type, flags, extra
	require.NoError(t, err)
	require.Equal(t, 4, n)
	n, err = conn.Write([]byte{0, 0, 0, 0, 0, 0, 0, 0}) // Unused single-word request payload
	require.NoError(t, err)
	require.Equal(t, 8, n)

	// Read the response
	buf := make([]byte, 64)
	n, err = conn.Read(buf)
	require.NoError(t, err)

	require.NoError(t, conn.Close())

	require.NoError(t, server.Stop())

	err = <-ch
	assert.NoError(t, err)
}

func TestServer_ConcurrentHandleAndClose(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	cluster := newTestCluster()

	server, err := bindings.NewServer(cluster)
	require.NoError(t, err)

	defer server.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	runCh := make(chan error)
	go func() {
		err := server.Run()
		runCh <- err
	}()

	require.True(t, server.Ready())

	acceptCh := make(chan struct{})
	go func() {
		conn, err := listener.Accept()
		require.NoError(t, err)
		server.Handle(conn)
		acceptCh <- struct{}{}
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)

	require.NoError(t, conn.Close())

	require.NoError(t, server.Stop())

	assert.NoError(t, <-runCh)
	<-acceptCh
}

type testCluster struct {
}

func newTestCluster() *testCluster {
	return &testCluster{}
}

func (c *testCluster) Leader() string {
	return "127.0.0.1:666"
}

func (c *testCluster) Servers() ([]bindings.ServerInfo, error) {
	servers := []bindings.ServerInfo{
		{ID: 1, Address: "1.2.3.4:666"},
		{ID: 2, Address: "5.6.7.8:666"},
	}

	return servers, nil
}

func (c *testCluster) Register(*bindings.Conn) {
}

func (c *testCluster) Unregister(*bindings.Conn) {
}

func (c *testCluster) Barrier() error {
	return nil
}

func (c *testCluster) Recover(token uint64) error {
	return nil
}

func (c *testCluster) Checkpoint(*bindings.Conn) error {
	return nil
}
