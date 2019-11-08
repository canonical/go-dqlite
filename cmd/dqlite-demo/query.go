package main

import (
	"database/sql"
	"fmt"

	"github.com/canonical/go-dqlite/driver"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// Return a new query key command.
func newQuery() *cobra.Command {
	var cluster *[]string

	cmd := &cobra.Command{
		Use:   "query <key>",
		Short: "Get a key from the demo table.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			key := args[0]
			store := getStore(*cluster)
			driver, err := driver.New(store, driver.WithLogFunc(logFunc))
			if err != nil {
				return errors.Wrapf(err, "failed to create dqlite driver")
			}
			sql.Register("dqlite", driver)

			db, err := sql.Open("dqlite", "demo.db")
			if err != nil {
				return errors.Wrap(err, "can't open demo database")
			}
			defer db.Close()

			if _, err := db.Exec("CREATE TABLE IF NOT EXISTS model (key TEXT, value TEXT)"); err != nil {
				return errors.Wrap(err, "can't create demo table")
			}

			row := db.QueryRow("SELECT value FROM model WHERE key = ?", key)
			value := ""
			if err := row.Scan(&value); err != nil {
				return errors.Wrap(err, "failed to get key")
			}
			fmt.Println(value)

			return nil
		},
	}

	flags := cmd.Flags()
	cluster = flags.StringSliceP("cluster", "c", defaultCluster, "addresses of existing cluster nodes")

	return cmd
}
