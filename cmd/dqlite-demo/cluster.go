package main

import (
	"context"
	"fmt"
	"time"

	dqclient "github.com/canonical/go-dqlite/client"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var (
	defaultCluster = []string{
		"127.0.0.1:9181",
		"127.0.0.1:9182",
		"127.0.0.1:9183",
	}
)

// Return a cluster nodes command.
func newCluster() *cobra.Command {
	var cluster *[]string

	cmd := &cobra.Command{
		Use:   "cluster",
		Short: "display cluster nodes.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			client, err := getLeader(ctx, *cluster)
			if err != nil {
				return errors.Wrap(err, "can't connect to cluster leader")
			}
			defer client.Close()

			var leader *dqclient.NodeInfo
			var nodes []dqclient.NodeInfo
			if leader, err = client.Leader(ctx); err != nil {
				return errors.Wrap(err, "can't get leader")
			}

			if nodes, err = client.Cluster(ctx); err != nil {
				return errors.Wrap(err, "can't get cluster")
			}

			fmt.Printf("ID \tLeader \tAddress\n")
			for _, node := range nodes {
				fmt.Printf("%d \t%v \t%s\n", node.ID, node.ID == leader.ID, node.Address)
			}
			return nil
		},
	}

	flags := cmd.Flags()
	cluster = flags.StringSliceP("cluster", "c", defaultCluster, "addresses of existing cluster nodes")

	return cmd
}
