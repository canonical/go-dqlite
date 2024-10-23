package benchmark_test

import (
	"context"
	"database/sql"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/canonical/go-dqlite/app"
	"github.com/canonical/go-dqlite/benchmark"
	"github.com/stretchr/testify/require"
)

const (
	addr1 = "127.0.0.1:9011"
	addr2 = "127.0.0.1:9012"
	addr3 = "127.0.0.1:9013"
)

func bmSetup(t *testing.T, addr string, join []string) (string, *app.App, *sql.DB, func()) {
	t.Helper()

	dir, err := ioutil.TempDir("", "dqlite-app-test-")
	require.NoError(t, err)

	app, err := app.New(dir, app.WithAddress(addr), app.WithCluster(join))
	require.NoError(t, err)

	readyCtx, cancel := context.WithTimeout(context.Background(), time.Duration(3)*time.Second)
	err = app.Ready(readyCtx)
	require.NoError(t, err)

	db, err := app.Open(context.Background(), "benchmark")
	require.NoError(t, err)

	cleanups := func() {
		os.RemoveAll(dir)
		cancel()
	}
	return dir, app, db, cleanups
}

func bmRun(t *testing.T, bm *benchmark.Benchmark, app *app.App, db *sql.DB) {
	defer db.Close()
	defer app.Close()
	ch := make(chan os.Signal)

	err := bm.Run(ch)
	require.NoError(t, err)
}

// Create a Benchmark with default values.
func TestNew_Default(t *testing.T) {
	dir, app, db, cleanup := bmSetup(t, addr1, nil)
	defer cleanup()

	bm, err := benchmark.New(
		app,
		db,
		dir,
		benchmark.WithCluster([]string{addr1}),
		benchmark.WithDuration(1))
	require.NoError(t, err)

	bmRun(t, bm, app, db)
}

// Create a Benchmark with a kvReadWriteWorkload.
func TestNew_KvReadWrite(t *testing.T) {
	dir, app, db, cleanup := bmSetup(t, addr1, nil)
	defer cleanup()

	bm, err := benchmark.New(
		app,
		db,
		dir,
		benchmark.WithCluster([]string{addr1}),
		benchmark.WithDuration(1),
		benchmark.WithWorkload("KvReadWrite"))
	require.NoError(t, err)

	bmRun(t, bm, app, db)
}

// Create a clustered Benchmark.
func TestNew_ClusteredKvReadWrite(t *testing.T) {
	dir, app, db, cleanup := bmSetup(t, addr1, nil)
	_, _, _, cleanup2 := bmSetup(t, addr2, []string{addr1})
	_, _, _, cleanup3 := bmSetup(t, addr3, []string{addr1})
	defer cleanup()
	defer cleanup2()
	defer cleanup3()

	bm, err := benchmark.New(
		app,
		db,
		dir,
		benchmark.WithCluster([]string{addr1, addr2, addr3}),
		benchmark.WithDuration(2))
	require.NoError(t, err)

	bmRun(t, bm, app, db)
}

// Create a clustered Benchmark that times out waiting for the cluster to form.
func TestNew_ClusteredTimeout(t *testing.T) {
	dir, app, db, cleanup := bmSetup(t, addr1, nil)
	defer cleanup()
	defer db.Close()
	defer app.Close()

	bm, err := benchmark.New(
		app,
		db,
		dir,
		benchmark.WithCluster([]string{addr1, addr2}),
		benchmark.WithClusterTimeout(2))
	require.NoError(t, err)

	ch := make(chan os.Signal)
	err = bm.Run(ch)
	require.Errorf(t, err, "Timed out waiting for cluster: context deadline exceeded")
}
