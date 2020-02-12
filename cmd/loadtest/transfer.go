package main

import (
	"context"
	"log"
	"time"

	"github.com/canonical/go-dqlite/client"
)

func getLeader(cluster []string) (*client.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	store := getStore(ctx, cluster)
	return client.FindLeader(ctx, store)
}

func transfer(id uint64, cluster []string) error {
	client, err := getLeader(cluster)
	if err != nil {
		return err
	}
	defer client.Close()

	return client.Transfer(context.Background(), id)
}

func rotateLeadership(cluster []string) {
	client, err := getLeader(cluster)
	if err != nil {
		log.Fatalln("error getting client:", err)
	}
	nodes, err := client.Cluster(context.Background())
	if err != nil {
		log.Fatalln("can't get node info:", err)
	}

	skip := true // node 1 will be leader, don't start with it
	for {
		for _, node := range nodes {
			if skip {
				skip = false
				continue
			}
			if err = transfer(node.ID, cluster); err != nil {
				log.Printf("error transferring leadership to node: %d -- %v\n", node.ID, err)
			} else {
				log.Println("transferred leadership to node:", node.ID)
			}
			time.Sleep(time.Second)
		}
	}

}
