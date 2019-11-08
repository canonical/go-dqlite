package main

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/canonical/go-dqlite/driver"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// Return a new benchmark command.
func newBenchmark() *cobra.Command {
	var cluster *[]string

	cmd := &cobra.Command{
		Use:   "benchmark <n>",
		Short: "Run the given number of updates and print benchmark data.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			n, err := strconv.Atoi(args[0])
			if err != nil {
				return errors.Wrap(err, "invalid number")
			}
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

			var duration time.Duration
			for i := 0; i < n; i++ {
				value := fmt.Sprintf("%d", i)
				start := time.Now()
				if _, err := db.Exec("INSERT OR REPLACE INTO model(key, value) VALUES('bench', ?)", value); err != nil {
					return errors.Wrap(err, "can't update key")
				}
				duration += time.Since(start)
			}
			fmt.Printf("update took %s on average\n", duration/time.Duration(n))

			return nil
		},
	}

	flags := cmd.Flags()
	cluster = flags.StringSliceP("cluster", "c", defaultCluster, "addresses of existing cluster nodes")

	return cmd
}
