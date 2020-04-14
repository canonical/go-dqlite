package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/go-dqlite/internal/shell"
	"github.com/peterh/liner"
	"github.com/spf13/cobra"
)

func main() {
	var cluster *[]string

	cmd := &cobra.Command{
		Use:   "dqlite -c <cluster> <database>",
		Short: "Standard dqlite shell",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			servers := make([]client.NodeInfo, len(*cluster))
			for i, address := range *cluster {
				servers[i].Address = address
			}
			store := client.NewInmemNodeStore()
			store.Set(context.Background(), servers)
			sh, err := shell.New(args[0], store)
			if err != nil {
				return err
			}

			line := liner.NewLiner()
			defer line.Close()

			for {
				input, err := line.Prompt("dqlite> ")
				if err != nil {
					if err == io.EOF {
						break
					}
					return err
				}

				result, err := sh.Process(context.Background(), input)
				if err != nil {
					fmt.Println("Error: ", err)
				} else if result != "" {
					fmt.Println(result)
				}
			}

			return nil
		},
	}

	flags := cmd.Flags()
	cluster = flags.StringSliceP("cluster", "c", nil, "comma-separted list of db nodes")

	cmd.MarkFlagRequired("cluster")

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
