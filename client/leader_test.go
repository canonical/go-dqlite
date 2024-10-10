package client_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	dqlite "github.com/canonical/go-dqlite"
	"github.com/canonical/go-dqlite/client"
	"github.com/stretchr/testify/require"
)

// LeaderConnector recycles connections intelligently.
func TestLeaderConnector(t *testing.T) {
	infos, cleanup := setup(t)
	defer cleanup()

	store := client.NewInmemNodeStore()
	store.Set(context.Background(), infos[:1])

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	connector := client.NewLeaderConnector(store)

	cli, err := connector.Find(ctx)
	require.NoError(t, err)
	firstProto := reflect.ValueOf(cli).Elem().FieldByName("protocol").Pointer()
	require.NoError(t, cli.Close())

	cli, err = connector.Find(ctx)
	require.NoError(t, err)
	secondProto := reflect.ValueOf(cli).Elem().FieldByName("protocol").Pointer()

	// The first leader connection was returned to the leader tracker
	// when we closed the client, and is returned by the second call
	// to FindLeader.
	require.Equal(t, firstProto, secondProto)

	// The reused connection is good to go.
	err = cli.Add(ctx, infos[1])
	require.NoError(t, err)

	// Trigger an unsuccessful Protocol operation that will cause the
	// client's connection to be marked as unsuitable for reuse.
	shortCtx, cancel := context.WithDeadline(context.Background(), time.Unix(1, 0))
	cancel()
	_, err = cli.Cluster(shortCtx)
	require.Error(t, err)
	require.NoError(t, cli.Close())

	cli, err = connector.Find(ctx)
	require.NoError(t, err)
	thirdProto := reflect.ValueOf(cli).Elem().FieldByName("protocol").Pointer()
	require.NoError(t, cli.Close())

	// The previous connection was not reused.
	require.NotEqual(t, secondProto, thirdProto)
}

func TestMembership(t *testing.T) {
	infos, cleanup := setup(t)
	defer cleanup()

	store := client.NewInmemNodeStore()
	store.Set(context.Background(), infos[:1])

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	client, err := client.FindLeader(ctx, store)
	require.NoError(t, err)
	defer client.Close()

	err = client.Add(ctx, infos[1])
	require.NoError(t, err)
}

func setup(t *testing.T) ([]client.NodeInfo, func()) {
	n := 3
	nodes := make([]*dqlite.Node, n)
	infos := make([]client.NodeInfo, n)
	var cleanups []func()

	for i := range nodes {
		id := uint64(i + 1)
		address := fmt.Sprintf("@test-%d", id)
		dir, cleanup := newDir(t)
		cleanups = append(cleanups, cleanup)
		node, err := dqlite.New(id, address, dir, dqlite.WithBindAddress(address))
		require.NoError(t, err)
		nodes[i] = node
		infos[i].ID = id
		infos[i].Address = address
		err = node.Start()
		require.NoError(t, err)
		cleanups = append(cleanups, func() { node.Close() })
	}

	return infos, func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}
}
