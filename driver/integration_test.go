package driver_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	dqlite "github.com/canonical/go-dqlite"
	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/go-dqlite/driver"
	"github.com/canonical/go-dqlite/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// https://sqlite.org/rescode.html#constraint_unique
const SQLITE_CONSTRAINT_UNIQUE = 2067

func TestIntegration_DatabaseSQL(t *testing.T) {
	db, _, cleanup := newDB(t, 3)
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
}

func TestIntegration_ConstraintError(t *testing.T) {
	db, _, cleanup := newDB(t, 3)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE test (n INT, UNIQUE (n))")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO test (n) VALUES (1)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO test (n) VALUES (1)")
	if err, ok := err.(driver.Error); ok {
		assert.Equal(t, SQLITE_CONSTRAINT_UNIQUE, err.Code)
		assert.Equal(t, "UNIQUE constraint failed: test.n", err.Message)
	} else {
		t.Fatalf("expected diver error, got %+v", err)
	}
}

func TestIntegration_ExecBindError(t *testing.T) {
	db, _, cleanup := newDB(t, 1)
	defer cleanup()
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := db.ExecContext(ctx, "CREATE TABLE test (n INT)")
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, "INSERT INTO test(n) VALUES(1)", 1)
	assert.EqualError(t, err, "bind parameters")
}

func TestIntegration_QueryBindError(t *testing.T) {
	db, _, cleanup := newDB(t, 1)
	defer cleanup()
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := db.QueryContext(ctx, "SELECT 1", 1)
	assert.EqualError(t, err, "bind parameters")
}

func TestIntegration_LargeQuery(t *testing.T) {
	db, _, cleanup := newDB(t, 3)
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
}

// Build a 2-node cluster, kill one node and recover the other.
func TestIntegration_Recover(t *testing.T) {
	db, helpers, cleanup := newDB(t, 2)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE test (n INT)")
	require.NoError(t, err)

	helpers[0].Close()
	helpers[1].Close()

	helpers[0].Create()

	infos := []client.NodeInfo{{ID: 1, Address: "@1"}}
	require.NoError(t, helpers[0].Node.Recover(infos))

	helpers[0].Start()

	// FIXME: this is necessary otherwise the INSERT below fails with "no
	// such table", because the replication hooks are not triggered and the
	// barrier is not applied.
	_, err = db.Exec("CREATE TABLE test2 (n INT)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO test(n) VALUES(1)")
	require.NoError(t, err)
}

// The db.Ping() method can be used to wait until there is a stable leader.
func TestIntegration_PingOnlyWorksOnceLeaderElected(t *testing.T) {
	db, helpers, cleanup := newDB(t, 2)
	defer cleanup()

	helpers[0].Close()

	// Ping returns an error, since the cluster is not available.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	assert.Error(t, db.PingContext(ctx))

	helpers[0].Create()
	helpers[0].Start()

	// Ping now returns no error, since the cluster is available.
	assert.NoError(t, db.Ping())

	// If leadership is lost after the first successful call, Ping() still
	// returns no error.
	helpers[0].Close()
	assert.NoError(t, db.Ping())
}

func TestIntegration_HighAvailability(t *testing.T) {
	db, helpers, cleanup := newDB(t, 3)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE test (n INT)")
	require.NoError(t, err)

	// Shutdown all three nodes.
	helpers[0].Close()
	helpers[1].Close()
	helpers[2].Close()

	// Restart two of them.
	helpers[1].Create()
	helpers[2].Create()
	helpers[1].Start()
	helpers[2].Start()

	// Give the cluster a chance to establish a quorom
	time.Sleep(2 * time.Second)

	_, err = db.Exec("INSERT INTO test(n) VALUES(1)")
	require.NoError(t, err)
}

func TestIntegration_LeadershipTransfer(t *testing.T) {
	db, helpers, cleanup := newDB(t, 3)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE test (n INT)")
	require.NoError(t, err)

	cli := helpers[0].Client()
	require.NoError(t, cli.Transfer(context.Background(), 2))

	_, err = db.Exec("INSERT INTO test(n) VALUES(1)")
	require.NoError(t, err)
}

func TestIntegration_LeadershipTransfer_Tx(t *testing.T) {
	db, helpers, cleanup := newDB(t, 3)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE test (n INT)")
	require.NoError(t, err)

	cli := helpers[0].Client()
	require.NoError(t, cli.Transfer(context.Background(), 2))

	tx, err := db.Begin()
	require.NoError(t, err)

	_, err = tx.Query("SELECT * FROM test")
	require.NoError(t, err)

	require.NoError(t, tx.Commit())
}

func TestOptions(t *testing.T) {
	// make sure applying all options doesn't break anything
	store := client.NewInmemNodeStore()
	log := logging.Test(t)
	_, err := driver.New(
		store,
		driver.WithLogFunc(log),
		driver.WithContext(context.Background()),
		driver.WithConnectionTimeout(15*time.Second),
		driver.WithContextTimeout(2*time.Second),
		driver.WithConnectionBackoffFactor(50*time.Millisecond),
		driver.WithConnectionBackoffCap(1*time.Second),
		driver.WithAttemptTimeout(5*time.Second),
		driver.WithRetryLimit(0),
	)
	require.NoError(t, err)
}

func newDB(t *testing.T, n int) (*sql.DB, []*nodeHelper, func()) {
	infos := make([]client.NodeInfo, n)
	for i := range infos {
		infos[i].ID = uint64(i + 1)
		infos[i].Address = fmt.Sprintf("@%d", infos[i].ID)
		infos[i].Role = client.Voter
	}
	return newDBWithInfos(t, infos)
}

func newDBWithInfos(t *testing.T, infos []client.NodeInfo) (*sql.DB, []*nodeHelper, func()) {
	helpers, helpersCleanup := newNodeHelpers(t, infos)

	store := client.NewInmemNodeStore()

	require.NoError(t, store.Set(context.Background(), infos))

	log := logging.Test(t)

	driver, err := driver.New(store, driver.WithLogFunc(log))
	require.NoError(t, err)

	driverName := fmt.Sprintf("dqlite-integration-test-%d", driversCount)
	sql.Register(driverName, driver)

	driversCount++

	db, err := sql.Open(driverName, "test.db")
	require.NoError(t, err)

	cleanup := func() {
		require.NoError(t, db.Close())
		helpersCleanup()
	}

	return db, helpers, cleanup
}

func registerDriver(driver *driver.Driver) string {
	name := fmt.Sprintf("dqlite-integration-test-%d", driversCount)
	sql.Register(name, driver)
	driversCount++
	return name
}

type nodeHelper struct {
	t       *testing.T
	ID      uint64
	Address string
	Dir     string
	Node    *dqlite.Node
}

func newNodeHelper(t *testing.T, id uint64, address string) *nodeHelper {
	h := &nodeHelper{
		t:       t,
		ID:      id,
		Address: address,
	}

	h.Dir, _ = newDir(t)

	h.Create()
	h.Start()

	return h
}

func (h *nodeHelper) Client() *client.Client {
	client, err := client.New(context.Background(), h.Node.BindAddress())
	require.NoError(h.t, err)
	return client
}

func (h *nodeHelper) Create() {
	var err error
	require.Nil(h.t, h.Node)
	h.Node, err = dqlite.New(h.ID, h.Address, h.Dir, dqlite.WithBindAddress(h.Address))
	require.NoError(h.t, err)
}

func (h *nodeHelper) Start() {
	require.NotNil(h.t, h.Node)
	require.NoError(h.t, h.Node.Start())
}

func (h *nodeHelper) Close() {
	require.NotNil(h.t, h.Node)
	require.NoError(h.t, h.Node.Close())
	h.Node = nil
}

func (h *nodeHelper) cleanup() {
	if h.Node != nil {
		h.Close()
	}
	require.NoError(h.t, os.RemoveAll(h.Dir))
}

func newNodeHelpers(t *testing.T, infos []client.NodeInfo) ([]*nodeHelper, func()) {
	t.Helper()

	n := len(infos)
	helpers := make([]*nodeHelper, n)

	for i, info := range infos {
		helpers[i] = newNodeHelper(t, info.ID, info.Address)

		if i > 0 {
			client := helpers[0].Client()
			defer client.Close()

			require.NoError(t, client.Add(context.Background(), infos[i]))
		}
	}

	cleanup := func() {
		for _, helper := range helpers {
			helper.cleanup()
		}
	}

	return helpers, cleanup
}

var driversCount = 0

func TestIntegration_ColumnTypeName(t *testing.T) {
	db, _, cleanup := newDB(t, 1)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE test (n INT, UNIQUE (n))")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO test (n) VALUES (1)")
	require.NoError(t, err)

	rows, err := db.Query("SELECT n FROM test")
	require.NoError(t, err)
	defer rows.Close()

	types, err := rows.ColumnTypes()
	require.NoError(t, err)

	assert.Equal(t, "INTEGER", types[0].DatabaseTypeName())

	require.True(t, rows.Next())
	var n int64
	err = rows.Scan(&n)
	require.NoError(t, err)

	assert.Equal(t, int64(1), n)
}

func TestIntegration_SqlNullTime(t *testing.T) {
	db, _, cleanup := newDB(t, 1)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE test (tm DATETIME)")
	require.NoError(t, err)

	// Insert sql.NullTime into DB
	var t1 sql.NullTime
	res, err := db.Exec("INSERT INTO test (tm) VALUES (?)", t1)
	require.NoError(t, err)

	n, err := res.RowsAffected()
	require.NoError(t, err)
	assert.EqualValues(t, n, 1)

	// Retrieve inserted sql.NullTime from DB
	row := db.QueryRow("SELECT tm FROM test LIMIT 1")
	var t2 sql.NullTime
	err = row.Scan(&t2)
	require.NoError(t, err)

	assert.Equal(t, t1, t2)
}
