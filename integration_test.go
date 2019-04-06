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
		id := uint(i + 1)
		dir, dirCleanup := newDir(t)
		server, err := dqlite.NewServer(id, dir, listener)
		require.NoError(t, err)

		cleanups = append(cleanups, func() {
			require.NoError(t, server.Close())
			dirCleanup()
		})

		err = server.Bootstrap(infos)
		require.NoError(t, err)

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
