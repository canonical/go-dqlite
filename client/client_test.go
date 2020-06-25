package client_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	dqlite "github.com/canonical/go-dqlite"
	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/go-dqlite/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient_Leader(t *testing.T) {
	node, cleanup := newNode(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	client, err := client.New(ctx, node.BindAddress())
	require.NoError(t, err)
	defer client.Close()

	leader, err := client.Leader(context.Background())
	require.NoError(t, err)

	assert.Equal(t, leader.ID, uint64(1))
	assert.Equal(t, leader.Address, "@1001")
}

func TestClient_Dump(t *testing.T) {
	node, cleanup := newNode(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	client, err := client.New(ctx, node.BindAddress())
	require.NoError(t, err)
	defer client.Close()

	// Open a database and create a test table.
	request := protocol.Message{}
	request.Init(4096)

	response := protocol.Message{}
	response.Init(4096)

	protocol.EncodeOpen(&request, "test.db", 0, "volatile")

	p := client.Protocol()
	err = p.Call(ctx, &request, &response)
	require.NoError(t, err)

	db, err := protocol.DecodeDb(&response)
	require.NoError(t, err)

	protocol.EncodeExecSQL(&request, uint64(db), "CREATE TABLE foo (n INT)", nil)

	err = p.Call(ctx, &request, &response)
	require.NoError(t, err)

	files, err := client.Dump(ctx, "test.db")
	require.NoError(t, err)

	require.Len(t, files, 2)
	assert.Equal(t, "test.db", files[0].Name)
	assert.Equal(t, 4096, len(files[0].Data))

	assert.Equal(t, "test.db-wal", files[1].Name)
	assert.Equal(t, 8272, len(files[1].Data))
}

func TestClient_Cluster(t *testing.T) {
	node, cleanup := newNode(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	cli, err := client.New(ctx, node.BindAddress())
	require.NoError(t, err)
	defer cli.Close()

	servers, err := cli.Cluster(context.Background())
	require.NoError(t, err)

	assert.Len(t, servers, 1)
	assert.Equal(t, servers[0].ID, uint64(1))
	assert.Equal(t, servers[0].Address, "@1001")
	assert.Equal(t, servers[0].Role, client.Voter)
}

func TestClient_Transfer(t *testing.T) {
	node1, cleanup := newNode(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cli, err := client.New(ctx, node1.BindAddress())
	require.NoError(t, err)
	defer cli.Close()

	node2, cleanup := addNode(t, cli, 2)
	defer cleanup()

	err = cli.Assign(context.Background(), 2, client.Voter)
	require.NoError(t, err)

	err = cli.Transfer(context.Background(), 2)
	require.NoError(t, err)

	leader, err := cli.Leader(context.Background())
	require.NoError(t, err)
	assert.Equal(t, leader.ID, uint64(2))

	cli, err = client.New(ctx, node2.BindAddress())
	require.NoError(t, err)
	defer cli.Close()

	leader, err = cli.Leader(context.Background())
	require.NoError(t, err)
	assert.Equal(t, leader.ID, uint64(2))

}

func TestClient_Describe(t *testing.T) {
	node, cleanup := newNode(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	cli, err := client.New(ctx, node.BindAddress())
	require.NoError(t, err)
	defer cli.Close()

	metadata, err := cli.Describe(context.Background())
	require.NoError(t, err)

	assert.Equal(t, uint64(0), metadata.FailureDomain)
	assert.Equal(t, uint64(0), metadata.Weight)

	require.NoError(t, cli.Weight(context.Background(), 123))

	metadata, err = cli.Describe(context.Background())
	require.NoError(t, err)

	assert.Equal(t, uint64(0), metadata.FailureDomain)
	assert.Equal(t, uint64(123), metadata.Weight)
}

func newNode(t *testing.T) (*dqlite.Node, func()) {
	t.Helper()
	dir, dirCleanup := newDir(t)

	id := uint64(1)
	address := fmt.Sprintf("@%d", id+1000)
	node, err := dqlite.New(uint64(1), address, dir, dqlite.WithBindAddress(address))
	require.NoError(t, err)

	err = node.Start()
	require.NoError(t, err)

	cleanup := func() {
		require.NoError(t, node.Close())
		dirCleanup()
	}

	return node, cleanup
}

func addNode(t *testing.T, cli *client.Client, id uint64) (*dqlite.Node, func()) {
	t.Helper()
	dir, dirCleanup := newDir(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	address := fmt.Sprintf("@%d", id+1000)
	node, err := dqlite.New(id, address, dir, dqlite.WithBindAddress(address))
	require.NoError(t, err)

	err = node.Start()
	require.NoError(t, err)

	info := client.NodeInfo{
		ID:      id,
		Address: address,
		Role:    client.Spare,
	}

	err = cli.Add(ctx, info)
	require.NoError(t, err)

	cleanup := func() {
		require.NoError(t, node.Close())
		dirCleanup()
	}

	return node, cleanup
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
