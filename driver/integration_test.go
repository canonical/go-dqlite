package driver_test

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"testing"
	"time"

	dqlite "github.com/canonical/go-dqlite"
	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/go-dqlite/driver"
	"github.com/canonical/go-dqlite/internal/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_DatabaseSQL(t *testing.T) {
	db, _, cleanup := newDB(t)
	defer cleanup()

	tx, err := db.Begin()
	require.NoError(t, err)

	_, err = tx.Exec(`
CREATE TABLE test  (n INT, s TEXT);
CREATE TABLE test2 (n INT, t DATETIME DEFAULT CURRENT_TIMESTAMP)
`)
	require.NoError(t, err)

	stmt, err := tx.Prepare("INSERT INTO test(n, s) VALUES(?, ?)")
	require.NoError(t, err)

	_, err = stmt.Exec(int64(123), "hello")
	require.NoError(t, err)

	require.NoError(t, stmt.Close())

	_, err = tx.Exec("INSERT INTO test2(n) VALUES(?)", int64(456))
	require.NoError(t, err)

	require.NoError(t, tx.Commit())

	tx, err = db.Begin()
	require.NoError(t, err)

	rows, err := tx.Query("SELECT n, s FROM test")
	require.NoError(t, err)

	for rows.Next() {
		var n int64
		var s string

		require.NoError(t, rows.Scan(&n, &s))

		assert.Equal(t, int64(123), n)
		assert.Equal(t, "hello", s)
	}

	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	rows, err = tx.Query("SELECT n, t FROM test2")
	require.NoError(t, err)

	for rows.Next() {
		var n int64
		var s time.Time

		require.NoError(t, rows.Scan(&n, &s))

		assert.Equal(t, int64(456), n)
	}

	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	require.NoError(t, tx.Rollback())

	require.NoError(t, db.Close())
}

func TestIntegration_LargeQuery(t *testing.T) {
	db, _, cleanup := newDB(t)
	defer cleanup()

	tx, err := db.Begin()
	require.NoError(t, err)

	_, err = tx.Exec("CREATE TABLE test (n INT)")
	require.NoError(t, err)

	stmt, err := tx.Prepare("INSERT INTO test(n) VALUES(?)")
	require.NoError(t, err)

	for i := 0; i < 512; i++ {
		_, err = stmt.Exec(int64(i))
		require.NoError(t, err)
	}

	require.NoError(t, stmt.Close())

	require.NoError(t, tx.Commit())

	tx, err = db.Begin()
	require.NoError(t, err)

	rows, err := tx.Query("SELECT n FROM test")
	require.NoError(t, err)

	columns, err := rows.Columns()
	require.NoError(t, err)

	assert.Equal(t, []string{"n"}, columns)

	count := 0
	for i := 0; rows.Next(); i++ {
		var n int64

		require.NoError(t, rows.Scan(&n))

		assert.Equal(t, int64(i), n)
		count++
	}

	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	assert.Equal(t, count, 512)

	require.NoError(t, tx.Rollback())

	require.NoError(t, db.Close())
}

func TestMembership(t *testing.T) {
	n := 3
	servers := make([]*dqlite.Node, n)
	var leaderInfo client.NodeInfo

	for i := range servers {
		id := uint64(i + 1)
		info := client.NodeInfo{ID: id, Address: fmt.Sprintf("%d", id)}
		dir, cleanup := newDir(t)
		defer cleanup()
		server, err := dqlite.NewNode(
			info, dir, dqlite.WithNodeDialFunc(dialFunc), dqlite.WithNodeLogFunc(logging.Test(t)))
		require.NoError(t, err)
		servers[i] = server
		if i == 0 {
			leaderInfo = info
		}
		err = server.Start()
		require.NoError(t, err)
		defer server.Close()
	}

	store := client.NewInmemNodeStore()
	store.Set(context.Background(), []client.NodeInfo{leaderInfo})
	server := servers[1]
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := server.Join(ctx, store, dialFunc)
	require.NoError(t, err)
}

func dialFunc(ctx context.Context, address string) (net.Conn, error) {
	return net.Dial("unix", fmt.Sprintf("@dqlite-%s", address))
}

func newDB(t *testing.T) (*sql.DB, []*dqlite.Node, func()) {
	n := 3

	infos := make([]client.NodeInfo, n)
	for i := range infos {
		infos[i].ID = uint64(i + 1)
		infos[i].Address = fmt.Sprintf("%d", infos[i].ID)
	}

	servers, cleanup := newNodes(t, infos)

	store, err := client.DefaultNodeStore(":memory:")
	require.NoError(t, err)

	require.NoError(t, store.Set(context.Background(), infos))

	log := logging.Test(t)
	driver, err := driver.New(store, driver.WithDialFunc(dialFunc), driver.WithLogFunc(log))
	require.NoError(t, err)

	driverName := fmt.Sprintf("dqlite-integration-test-%d", driversCount)
	sql.Register(driverName, driver)

	driversCount++

	db, err := sql.Open(driverName, "test.db")
	require.NoError(t, err)

	return db, servers, cleanup
}

func newNodes(t *testing.T, infos []client.NodeInfo) ([]*dqlite.Node, func()) {
	t.Helper()

	n := len(infos)
	servers := make([]*dqlite.Node, n)
	cleanups := make([]func(), 0)

	for i, info := range infos {
		dir, dirCleanup := newDir(t)
		server, err := dqlite.NewNode(
			info, dir, dqlite.WithNodeDialFunc(dialFunc),
			dqlite.WithNodeLogFunc(logging.Test(t)))
		require.NoError(t, err)

		cleanups = append(cleanups, func() {
			require.NoError(t, server.Close())
			dirCleanup()
		})

		err = server.Start()
		require.NoError(t, err)

		servers[i] = server

	}

	cleanup := func() {
		for _, f := range cleanups {
			f()
		}
	}

	return servers, cleanup
}

var driversCount = 0
