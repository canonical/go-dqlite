package client_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	dqlite "github.com/canonical/go-dqlite/v3"
	"github.com/canonical/go-dqlite/v3/client"
	"github.com/stretchr/testify/require"
)

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
