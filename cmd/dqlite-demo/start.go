package main

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	dqlite "github.com/canonical/go-dqlite"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// Return a new start command.
func newStart() *cobra.Command {
	var dir string
	var address string

	cmd := &cobra.Command{
		Use:   "start <id>",
		Short: "Start a dqlite-demo node.",
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
			dir = filepath.Join(dir, args[0])
			if err := os.MkdirAll(dir, 0755); err != nil {
				return errors.Wrapf(err, "can't create %s", dir)
			}
			node, err := dqlite.New(
				uint64(id), address, dir,
				dqlite.WithBindAddress(address),
				dqlite.WithNetworkLatency(20*time.Millisecond),
			)
			if err != nil {
				return errors.Wrap(err, "failed to create node")
			}
			if err := node.Start(); err != nil {
				return errors.Wrap(err, "failed to start node")
			}

			ch := make(chan os.Signal)
			signal.Notify(ch, syscall.SIGINT)
			signal.Notify(ch, syscall.SIGTERM)
			<-ch

			if err := node.Close(); err != nil {
				return errors.Wrap(err, "failed to stop node")
			}

			return nil
		},
	}

	flags := cmd.Flags()
	flags.StringVarP(&dir, "dir", "d", "/tmp/dqlite-demo", "base demo directory")
	flags.StringVarP(&address, "address", "a", "", "address of the node (default is 127.0.0.1:918<ID>)")

	return cmd
}
