package dqlite_test

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"testing"
	"time"

	dqlite "github.com/CanonicalLtd/go-dqlite"
	"github.com/CanonicalLtd/go-dqlite/internal/logging"
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
	listeners := make([]net.Listener, n)
	servers := make([]*dqlite.Server, n)
	var leaderInfo dqlite.ServerInfo

	for i := range listeners {
		id := uint64(i + 1)
		listener := newListener(t)
		info := dqlite.ServerInfo{ID: id, Address: listener.Addr().String()}
		dir, cleanup := newDir(t)
		defer cleanup()
		server, err := dqlite.NewServer(info, dir)
		require.NoError(t, err)
		listeners[i] = listener
		servers[i] = server
		if i == 0 {
			err := server.Bootstrap([]dqlite.ServerInfo{info})
			require.NoError(t, err)
			leaderInfo = info
		}
		err = server.Start(listener)
		require.NoError(t, err)
		defer server.Close()
	}

	server := servers[1]
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := server.Join(ctx, leaderInfo)
	require.NoError(t, err)
}

func newDB(t *testing.T) (*sql.DB, []*dqlite.Server, func()) {
	n := 3

	listeners := make([]net.Listener, n)
	infos := make([]dqlite.ServerInfo, n)
	for i := range listeners {
		listeners[i] = newListener(t)
		infos[i].ID = uint64(i + 1)
		infos[i].Address = listeners[i].Addr().String()
	}

	servers, cleanup := newServers(t, listeners, infos)

	store, err := dqlite.DefaultServerStore(":memory:")
	require.NoError(t, err)

	require.NoError(t, store.Set(context.Background(), infos))

	log := logging.Test(t)
	driver, err := dqlite.NewDriver(store, dqlite.WithLogFunc(log))
	require.NoError(t, err)

	driverName := fmt.Sprintf("dqlite-integration-test-%d", driversCount)
	sql.Register(driverName, driver)

	driversCount++

	db, err := sql.Open(driverName, "test.db")
	require.NoError(t, err)

	return db, servers, cleanup
}

func newServers(t *testing.T, listeners []net.Listener, infos []dqlite.ServerInfo) ([]*dqlite.Server, func()) {
	t.Helper()

	n := len(listeners)
	servers := make([]*dqlite.Server, n)
	cleanups := make([]func(), 0)

	for i, listener := range listeners {
		id := uint64(i + 1)
		dir, dirCleanup := newDir(t)
		info := dqlite.ServerInfo{ID: id, Address: listener.Addr().String()}
		server, err := dqlite.NewServer(info, dir)
		require.NoError(t, err)

		cleanups = append(cleanups, func() {
			require.NoError(t, server.Close())
			dirCleanup()
		})

		err = server.Bootstrap(infos)
		require.NoError(t, err)

		err = server.Start(listener)
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
