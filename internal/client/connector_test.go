package client_test

import (
	"context"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"github.com/canonical/go-dqlite/internal/bindings"
	"github.com/canonical/go-dqlite/internal/client"
	"github.com/canonical/go-dqlite/internal/logging"
	"github.com/Rican7/retry/backoff"
	"github.com/Rican7/retry/strategy"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Successful connection.
func TestConnector_Connect_Success(t *testing.T) {
	address, cleanup := newServer(t, 0)
	defer cleanup()

	store := newStore(t, []string{address})

	connector := newConnector(t, store)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	client, err := connector.Connect(ctx)
	require.NoError(t, err)

	assert.NoError(t, client.Close())
}

// Connection failed because the server store is empty.
func TestConnector_Connect_Error_EmptyServerStore(t *testing.T) {
	store := newStore(t, []string{})

	connector := newConnector(t, store)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	_, err := connector.Connect(ctx)
	require.EqualError(t, err, "no available dqlite leader server found")
}

// Connection failed because the context was canceled.
func TestConnector_Connect_Error_AfterCancel(t *testing.T) {
	store := newStore(t, []string{"1.2.3.4:666"})

	connector := newConnector(t, store)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := connector.Connect(ctx)
	assert.EqualError(t, err, "no available dqlite leader server found")
}

// If an election is in progress, the connector will retry until a leader gets
// elected.
// func TestConnector_Connect_ElectionInProgress(t *testing.T) {
// 	address1, cleanup := newServer(t, 1)
// 	defer cleanup()

// 	address2, cleanup := newServer(t, 2)
// 	defer cleanup()

// 	address3, cleanup := newServer(t, 3)
// 	defer cleanup()

// 	store := newStore(t, []string{address1, address2, address3})

// 	connector := newConnector(t, store)

// 	go func() {
// 		// Simulate server 1 winning the election after 10ms
// 		time.Sleep(10 * time.Millisecond)
// 	}()

// 	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
// 	defer cancel()

// 	client, err := connector.Connect(ctx)
// 	require.NoError(t, err)

// 	assert.NoError(t, client.Close())
// }

// If a server reports that it knows about the leader, the hint will be taken
// and an attempt will be made to connect to it.
// func TestConnector_Connect_ServerKnowsAboutLeader(t *testing.T) {
// 	defer bindings.AssertNoMemoryLeaks(t)

// 	methods1 := &testClusterMethods{}
// 	methods2 := &testClusterMethods{}
// 	methods3 := &testClusterMethods{}

// 	address1, cleanup := newServer(t, 1, methods1)
// 	defer cleanup()

// 	address2, cleanup := newServer(t, 2, methods2)
// 	defer cleanup()

// 	address3, cleanup := newServer(t, 3, methods3)
// 	defer cleanup()

// 	// Server 1 will be contacted first, which will report that server 2 is
// 	// the leader.
// 	store := newStore(t, []string{address1, address2, address3})

// 	methods1.leader = address2
// 	methods2.leader = address2
// 	methods3.leader = address2

// 	connector := newConnector(t, store)

// 	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
// 	defer cancel()

// 	client, err := connector.Connect(ctx)
// 	require.NoError(t, err)

// 	assert.NoError(t, client.Close())
// }

// If a server reports that it knows about the leader, the hint will be taken
// and an attempt will be made to connect to it. If that leader has died, the
// next target will be tried.
// func TestConnector_Connect_ServerKnowsAboutDeadLeader(t *testing.T) {
// 	defer bindings.AssertNoMemoryLeaks(t)

// 	methods1 := &testClusterMethods{}
// 	methods2 := &testClusterMethods{}
// 	methods3 := &testClusterMethods{}

// 	address1, cleanup := newServer(t, 1, methods1)
// 	defer cleanup()

// 	address2, cleanup := newServer(t, 2, methods2)

// 	// Simulate server 2 crashing.
// 	cleanup()

// 	address3, cleanup := newServer(t, 3, methods3)
// 	defer cleanup()

// 	// Server 1 will be contacted first, which will report that server 2 is
// 	// the leader. However server 2 has crashed, and after a bit server 1
// 	// gets elected.
// 	store := newStore(t, []string{address1, address2, address3})
// 	methods1.leader = address2
// 	methods3.leader = address2

// 	go func() {
// 		// Simulate server 1 becoming the new leader after server 2
// 		// crashed.
// 		time.Sleep(10 * time.Millisecond)
// 		methods1.leader = address1
// 		methods3.leader = address1
// 	}()

// 	connector := newConnector(t, store)

// 	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
// 	defer cancel()

// 	client, err := connector.Connect(ctx)
// 	require.NoError(t, err)

// 	assert.NoError(t, client.Close())
// }

// If a server reports that it knows about the leader, the hint will be taken
// and an attempt will be made to connect to it. If that leader is not actually
// the leader the next target will be tried.
// func TestConnector_Connect_ServerKnowsAboutStaleLeader(t *testing.T) {
// 	defer bindings.AssertNoMemoryLeaks(t)

// 	methods1 := &testClusterMethods{}
// 	methods2 := &testClusterMethods{}
// 	methods3 := &testClusterMethods{}

// 	address1, cleanup := newServer(t, 1, methods1)
// 	defer cleanup()

// 	address2, cleanup := newServer(t, 2, methods2)
// 	defer cleanup()

// 	address3, cleanup := newServer(t, 3, methods3)
// 	defer cleanup()

// 	// Server 1 will be contacted first, which will report that server 2 is
// 	// the leader. However server 2 thinks that 3 is the leader, and server
// 	// 3 is actually the leader.
// 	store := newStore(t, []string{address1, address2, address3})
// 	methods1.leader = address2
// 	methods2.leader = address3
// 	methods3.leader = address3

// 	connector := newConnector(t, store)

// 	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
// 	defer cancel()

// 	client, err := connector.Connect(ctx)
// 	require.NoError(t, err)

// 	assert.NoError(t, client.Close())
// }

func newConnector(t *testing.T, store client.ServerStore) *client.Connector {
	t.Helper()

	config := client.Config{
		Dial:           client.UnixDial,
		AttemptTimeout: 100 * time.Millisecond,
		RetryStrategies: []strategy.Strategy{
			strategy.Backoff(backoff.BinaryExponential(time.Millisecond)),
		},
	}

	log := logging.Test(t)

	connector := client.NewConnector(0, store, config, log)

	return connector
}

// Create a new in-memory server store populated with the given addresses.
func newStore(t *testing.T, addresses []string) client.ServerStore {
	t.Helper()

	servers := make([]client.ServerInfo, len(addresses))
	for i, address := range addresses {
		servers[i].ID = uint64(i)
		servers[i].Address = address
	}

	store := client.NewInmemServerStore()
	require.NoError(t, store.Set(context.Background(), servers))

	return store
}

func newServer(t *testing.T, index int) (string, func()) {
	t.Helper()

	id := uint(index + 1)
	dir, dirCleanup := newDir(t)

	listener := newListener(t)
	address := listener.Addr().String()

	server, err := bindings.NewServer(id, address, dir, nil)
	require.NoError(t, err)

	acceptCh := make(chan error)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				acceptCh <- nil
				return
			}

			err = server.Handle(conn)
			if err == bindings.ErrServerStopped {
				acceptCh <- nil
				return
			}
			if err != nil {
				acceptCh <- err
				return
			}
		}
	}()

	cleanup := func() {
		require.NoError(t, listener.Close())
		require.NoError(t, server.Close())

		// Wait for the accept goroutine to exit.
		select {
		case err := <-acceptCh:
			assert.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("accept goroutine did not stop within a second")
		}

		dirCleanup()
	}

	return address, cleanup
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

func newListener(t *testing.T) net.Listener {
	t.Helper()

	listener, err := net.Listen("unix", "")
	require.NoError(t, err)

	return listener
}

func init() {
	err := bindings.Init()
	if err != nil {
		panic(errors.Wrap(err, "failed to initialize dqlite"))
	}
}
