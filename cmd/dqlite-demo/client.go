package main

import (
	"context"

	"github.com/canonical/go-dqlite/client"
)

func getLeader(ctx context.Context, cluster []string) (*client.Client, error) {
	return client.FindLeader(ctx, getStore(cluster))

}

func getStore(cluster []string) client.NodeStore {
	store := client.NewInmemNodeStore()
	if len(cluster) == 0 {
		cluster = defaultCluster
	}
	infos := make([]client.NodeInfo, 3)
	for i, address := range cluster {
		infos[i].ID = uint64(i + 1)
		infos[i].Address = address
	}
	store.Set(context.Background(), infos)
	return store
}
