package client_test

import (
	"context"
	"testing"
	"time"

	"github.com/CanonicalLtd/go-dqlite/internal/bindings"
	"github.com/CanonicalLtd/go-dqlite/internal/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient_Heartbeat(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	c, cleanup := newClient(t)
	defer cleanup()

	request, response := newMessagePair(512, 512)

	client.EncodeHeartbeat(&request, uint64(time.Now().Unix()))

	makeClientCall(t, c, &request, &response)

	servers, err := client.DecodeServers(&response)
	require.NoError(t, err)

	assert.Len(t, servers, 2)
	assert.Equal(t, client.Servers{
		{ID: uint64(1), Address: "1.2.3.4:666"},
		{ID: uint64(2), Address: "5.6.7.8:666"}},
		servers)
}

// Test sending a request that needs to be written into the dynamic buffer.
func TestClient_RequestWithDynamicBuffer(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	c, cleanup := newClient(t)
	defer cleanup()

	request, response := newMessagePair(64, 64)

	flags := uint64(bindings.OpenReadWrite | bindings.OpenCreate)
	client.EncodeOpen(&request, "test.db", flags, "test-0")

	makeClientCall(t, c, &request, &response)

	id, err := client.DecodeDb(&response)
	require.NoError(t, err)

	request.Reset()
	response.Reset()

	sql := `
CREATE TABLE foo (n INT);
CREATE TABLE bar (n INT);
CREATE TABLE egg (n INT);
CREATE TABLE baz (n INT);
`
	client.EncodeExecSQL(&request, uint64(id), sql, nil)

	makeClientCall(t, c, &request, &response)
}

func TestClient_Prepare(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	c, cleanup := newClient(t)
	defer cleanup()

	request, response := newMessagePair(64, 64)

	flags := uint64(bindings.OpenReadWrite | bindings.OpenCreate)
	client.EncodeOpen(&request, "test.db", flags, "test-0")

	makeClientCall(t, c, &request, &response)

	db, err := client.DecodeDb(&response)
	require.NoError(t, err)

	request.Reset()
	response.Reset()

	client.EncodePrepare(&request, uint64(db), "CREATE TABLE test (n INT)")

	makeClientCall(t, c, &request, &response)

	_, stmt, params, err := client.DecodeStmt(&response)
	require.NoError(t, err)

	assert.Equal(t, uint32(0), stmt)
	assert.Equal(t, uint64(0), params)
}

/*
func TestClient_Exec(t *testing.T) {
	client, cleanup := newClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	db, err := client.Open(ctx, "test.db", "volatile")
	require.NoError(t, err)

	stmt, err := client.Prepare(ctx, db.ID, "CREATE TABLE test (n INT)")
	require.NoError(t, err)

	_, err = client.Exec(ctx, db.ID, stmt.ID)
	require.NoError(t, err)
}

func TestClient_Query(t *testing.T) {
	client, cleanup := newClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	db, err := client.Open(ctx, "test.db", "volatile")
	require.NoError(t, err)

	start := time.Now()

	stmt, err := client.Prepare(ctx, db.ID, "CREATE TABLE test (n INT)")
	require.NoError(t, err)

	_, err = client.Exec(ctx, db.ID, stmt.ID)
	require.NoError(t, err)

	_, err = client.Finalize(ctx, db.ID, stmt.ID)
	require.NoError(t, err)

	stmt, err = client.Prepare(ctx, db.ID, "INSERT INTO test VALUES(1)")
	require.NoError(t, err)

	_, err = client.Exec(ctx, db.ID, stmt.ID)
	require.NoError(t, err)

	_, err = client.Finalize(ctx, db.ID, stmt.ID)
	require.NoError(t, err)

	stmt, err = client.Prepare(ctx, db.ID, "SELECT n FROM test")
	require.NoError(t, err)

	_, err = client.Query(ctx, db.ID, stmt.ID)
	require.NoError(t, err)

	_, err = client.Finalize(ctx, db.ID, stmt.ID)
	require.NoError(t, err)

	fmt.Printf("time %s\n", time.Since(start))
}
*/

func newClient(t *testing.T) (*client.Client, func()) {
	t.Helper()

	methods := &testClusterMethods{}

	address, serverCleanup := newServer(t, 0, methods)

	methods.leader = address

	store := newStore(t, []string{address})

	connector := newConnector(t, store)

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	client, err := connector.Connect(ctx)

	require.NoError(t, err)

	cleanup := func() {
		client.Close()
		serverCleanup()
	}

	return client, cleanup
}

// Perform a client call.
func makeClientCall(t *testing.T, c *client.Client, request, response *client.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	err := c.Call(ctx, request, response)
	require.NoError(t, err)
}

// Return a new message pair to be used as request and response.
func newMessagePair(size1, size2 int) (client.Message, client.Message) {
	message1 := client.Message{}
	message1.Init(size1)

	message2 := client.Message{}
	message2.Init(size2)

	return message1, message2
}
