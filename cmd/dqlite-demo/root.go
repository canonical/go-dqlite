package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

// Return a new root command.
func newRoot() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dqlite-demo",
		Short: "Demo application using dqlite",
		Long: `This demo shows how to integrate a Go application with dqlite.

Complete documentation is available at https://github.com/canonical/go-dqlite`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("not implemented")
		},
	}
	cmd.AddCommand(newStart())
	cmd.AddCommand(newAdd())
	cmd.AddCommand(newUpdate())
	cmd.AddCommand(newQuery())
	cmd.AddCommand(newBenchmark())
	cmd.AddCommand(newCluster())

	return cmd
}
