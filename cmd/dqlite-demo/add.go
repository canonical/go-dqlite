package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/canonical/go-dqlite/client"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// Return a new add command.
func newAdd() *cobra.Command {
	var address string
	var cluster *[]string

	cmd := &cobra.Command{
		Use:   "add <id>",
		Short: "Add a node to the dqlite-demo cluster.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := strconv.Atoi(args[0])
			if err != nil {
				return errors.Wrapf(err, "%s is not a number", args[0])
			}
			if id == 0 {
				return fmt.Errorf("ID must be greater than zero")
			}
			if address == "" {
				address = fmt.Sprintf("127.0.0.1:918%d", id)
			}
			info := client.NodeInfo{
				ID:      uint64(id),
				Address: address,
			}

			client, err := getLeader(*cluster)
			if err != nil {
				return errors.Wrap(err, "can't connect to cluster leader")
			}
			defer client.Close()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			if err := client.Add(ctx, info); err != nil {
				return errors.Wrap(err, "can't add node")
			}

			return nil
		},
	}

	flags := cmd.Flags()
	flags.StringVarP(&address, "address", "a", "", "address of the node to add (default is 127.0.0.1:918<ID>)")
	cluster = flags.StringSliceP("cluster", "c", defaultCluster, "addresses of existing cluster nodes")

	return cmd
}
