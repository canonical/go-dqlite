package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/canonical/go-dqlite/app"
	"github.com/canonical/go-dqlite/benchmark"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"
)

const (
	defaultClusterTimeout = 120
	defaultDir            = "/tmp/dqlite-benchmark"
	defaultDiskMode       = false
	defaultDriver         = false
	defaultDurationS      = 60
	defaultKvKeySize      = 32
	defaultKvValueSize    = 1024
	defaultWorkers        = 1
	defaultWorkload       = "kvwrite"
	docString             = "For benchmarking dqlite.\n\n" +
		"Run a 1 node benchmark:\n" +
		"dqlite-benchmark -d 127.0.0.1:9001 --driver --cluster 127.0.0.1:9001\n\n" +
		"Run a multi-node benchmark, the first node will self-elect and become leader,\n" +
		"the driver flag results in the workload being run from the first, leader node.\n" +
		"dqlite-benchmark --db 127.0.0.1:9001 --driver --cluster 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003 &\n" +
		"dqlite-benchmark --db 127.0.0.1:9002 --join 127.0.0.1:9001 &\n" +
		"dqlite-benchmark --db 127.0.0.1:9003 --join 127.0.0.1:9001 &\n\n" +
		"Run a multi-node benchmark, the first node will self-elect and become leader,\n" +
		"the driver flag results in the workload being run from the third, non-leader node.\n" +
		"dqlite-benchmark --db 127.0.0.1:9001 &\n" +
		"dqlite-benchmark --db 127.0.0.1:9002 --join 127.0.0.1:9001 &\n" +
		"dqlite-benchmark --db 127.0.0.1:9003 --join 127.0.0.1:9001 --driver --cluster 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003 &\n\n" +
		"The results can be found on the `driver` node in " + defaultDir + "/results or in the directory provided to the tool.\n" +
		"Benchmark results are files named `n-q-timestamp` where `n` is the number of the worker,\n" +
		"`q` is the type of query that was tracked. All results in the file are in milliseconds.\n"
)

func signalChannel() chan os.Signal {
	ch := make(chan os.Signal, 32)
	signal.Notify(ch, unix.SIGPWR)
	signal.Notify(ch, unix.SIGINT)
	signal.Notify(ch, unix.SIGQUIT)
	signal.Notify(ch, unix.SIGTERM)
	return ch
}

func main() {
	var cluster *[]string
	var clusterTimeout int
	var db string
	var dir string
	var driver bool
	var duration int
	var join *[]string
	var kvKeySize int
	var kvValueSize int
	var workers int
	var workload string
	var diskMode bool

	cmd := &cobra.Command{
		Use:   "dqlite-benchmark",
		Short: "For benchmarking dqlite",
		Long:  docString,
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := filepath.Join(dir, db)
			if err := os.MkdirAll(dir, 0755); err != nil {
				return errors.Wrapf(err, "can't create %s", dir)
			}

			app, err := app.New(dir, app.WithDiskMode(diskMode), app.WithAddress(db), app.WithCluster(*join))
			if err != nil {
				return err
			}

			readyCtx, cancel := context.WithTimeout(context.Background(), time.Duration(clusterTimeout)*time.Second)
			defer cancel()
			if err := app.Ready(readyCtx); err != nil {
				return errors.Wrap(err, "App not ready in time")
			}

			ch := signalChannel()
			if !driver {
				fmt.Println("Benchmark client ready. Send signal to abort or when done.")
				select {
				case <-ch:
					return nil
				}
			}

			if len(*cluster) == 0 {
				return fmt.Errorf("driver node, `--cluster` flag must be provided")
			}

			db, err := app.Open(context.Background(), "benchmark")
			if err != nil {
				return err
			}
			db.SetMaxOpenConns(500)
			db.SetMaxIdleConns(500)

			bm, err := benchmark.New(
				app,
				db,
				dir,
				benchmark.WithWorkload(workload),
				benchmark.WithDuration(duration),
				benchmark.WithWorkers(workers),
				benchmark.WithKvKeySize(kvKeySize),
				benchmark.WithKvValueSize(kvValueSize),
				benchmark.WithCluster(*cluster),
				benchmark.WithClusterTimeout(clusterTimeout),
			)
			if err != nil {
				return err
			}

			if err := bm.Run(ch); err != nil {
				return err
			}

			db.Close()
			app.Close()
			return nil
		},
	}

	flags := cmd.Flags()
	flags.StringVarP(&db, "db", "d", "", "Address used for internal database replication.")
	join = flags.StringSliceP("join", "j", nil, "Database addresses of existing nodes.")
	cluster = flags.StringSliceP("cluster", "c", nil, "Database addresses of all nodes taking part in the benchmark.\n"+
		"The driver will wait for all nodes to be online before running the benchmark.")
	flags.IntVar(&clusterTimeout, "cluster-timeout", defaultClusterTimeout, "How long the benchmark should wait in seconds for the whole cluster to be online.")
	flags.StringVarP(&dir, "dir", "D", defaultDir, "Data directory.")
	flags.StringVarP(&workload, "workload", "w", defaultWorkload, "The workload to run: \"kvwrite\" or \"kvreadwrite\".")
	flags.BoolVar(&driver, "driver", defaultDriver, "Set this flag to run the benchmark from this instance. Must be set on 1 node.")
	flags.IntVar(&duration, "duration", defaultDurationS, "Run duration in seconds.")
	flags.IntVar(&workers, "workers", defaultWorkers, "Number of workers executing the workload.")
	flags.IntVar(&kvKeySize, "key-size", defaultKvKeySize, "Size of the KV keys in bytes.")
	flags.IntVar(&kvValueSize, "value-size", defaultKvValueSize, "Size of the KV values in bytes.")
	flags.BoolVar(&diskMode, "disk", defaultDiskMode, "Warning: Unstable, Experimental. Set this flag to enable dqlite's disk-mode.")

	cmd.MarkFlagRequired("db")
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
