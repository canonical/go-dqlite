package main

import (
	"context"
	"time"

	"github.com/canonical/go-dqlite/client"
)

func getLeader(cluster []string) (*client.Client, error) {
	store := getStore(cluster)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	return client.FindLeader(ctx, store, client.WithLogFunc(logFunc))
}

func getStore(cluster []string) client.NodeStore {
	store := client.NewInmemNodeStore()
	if len(cluster) == 0 {
		cluster = []string{
			"127.0.0.1:9181",
			"127.0.0.1:9182",
			"127.0.0.1:9183",
		}
	}
	infos := make([]client.NodeInfo, 3)
	for i, address := range cluster {
		infos[i].ID = uint64(i + 1)
		infos[i].Address = address
	}
	store.Set(context.Background(), infos)
	return store
}
