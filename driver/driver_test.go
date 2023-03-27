// Copyright 2017 Canonical Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package driver_test

import (
	"context"
	"database/sql/driver"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	dqlite "github.com/canonical/go-dqlite"
	"github.com/canonical/go-dqlite/client"
	dqlitedriver "github.com/canonical/go-dqlite/driver"
	"github.com/canonical/go-dqlite/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDriver_Open(t *testing.T) {
	driver, cleanup := newDriver(t)
	defer cleanup()

	conn, err := driver.Open("test.db")
	require.NoError(t, err)

	assert.NoError(t, conn.Close())
}

func TestDriver_Prepare(t *testing.T) {
	driver, cleanup := newDriver(t)
	defer cleanup()

	conn, err := driver.Open("test.db")
	require.NoError(t, err)

	stmt, err := conn.Prepare("CREATE TABLE test (n INT)")
	require.NoError(t, err)

	assert.Equal(t, 0, stmt.NumInput())

	assert.NoError(t, conn.Close())
}

func TestConn_Exec(t *testing.T) {
	drv, cleanup := newDriver(t)
	defer cleanup()

	conn, err := drv.Open("test.db")
	require.NoError(t, err)

	_, err = conn.Begin()
	require.NoError(t, err)

	execer := conn.(driver.Execer)

	_, err = execer.Exec("CREATE TABLE test (n INT)", nil)
	require.NoError(t, err)

	result, err := execer.Exec("INSERT INTO test(n) VALUES(1)", nil)
	require.NoError(t, err)

	lastInsertID, err := result.LastInsertId()
	require.NoError(t, err)

	assert.Equal(t, lastInsertID, int64(1))

	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)

	assert.Equal(t, rowsAffected, int64(1))

	assert.NoError(t, conn.Close())
}

func TestConn_Query(t *testing.T) {
	drv, cleanup := newDriver(t)
	defer cleanup()

	conn, err := drv.Open("test.db")
	require.NoError(t, err)

	_, err = conn.Begin()
	require.NoError(t, err)

	execer := conn.(driver.Execer)

	_, err = execer.Exec("CREATE TABLE test (n INT)", nil)
	require.NoError(t, err)

	_, err = execer.Exec("INSERT INTO test(n) VALUES(1)", nil)
	require.NoError(t, err)

	queryer := conn.(driver.Queryer)

	_, err = queryer.Query("SELECT n FROM test", nil)
	require.NoError(t, err)

	assert.NoError(t, conn.Close())
}

func TestConn_QueryRow(t *testing.T) {
	drv, cleanup := newDriver(t)
	defer cleanup()

	conn, err := drv.Open("test.db")
	require.NoError(t, err)

	_, err = conn.Begin()
	require.NoError(t, err)

	execer := conn.(driver.Execer)

	_, err = execer.Exec("CREATE TABLE test (n INT)", nil)
	require.NoError(t, err)

	_, err = execer.Exec("INSERT INTO test(n) VALUES(1)", nil)
	require.NoError(t, err)

	_, err = execer.Exec("INSERT INTO test(n) VALUES(1)", nil)
	require.NoError(t, err)

	queryer := conn.(driver.Queryer)

	rows, err := queryer.Query("SELECT n FROM test", nil)
	require.NoError(t, err)

	values := make([]driver.Value, 1)
	require.NoError(t, rows.Next(values))

	require.NoError(t, rows.Close())

	assert.NoError(t, conn.Close())
}

func TestConn_QueryBlob(t *testing.T) {
	drv, cleanup := newDriver(t)
	defer cleanup()

	conn, err := drv.Open("test.db")
	require.NoError(t, err)

	_, err = conn.Begin()
	require.NoError(t, err)

	execer := conn.(driver.Execer)

	_, err = execer.Exec("CREATE TABLE test (data BLOB)", nil)
	require.NoError(t, err)

	values := []driver.Value{
		[]byte{'a', 'b', 'c'},
	}
	_, err = execer.Exec("INSERT INTO test(data) VALUES(?)", values)
	require.NoError(t, err)

	queryer := conn.(driver.Queryer)

	rows, err := queryer.Query("SELECT data FROM test", nil)
	require.NoError(t, err)

	assert.Equal(t, rows.Columns(), []string{"data"})

	values = make([]driver.Value, 1)
	require.NoError(t, rows.Next(values))

	assert.Equal(t, []byte{'a', 'b', 'c'}, values[0])

	assert.NoError(t, conn.Close())
}

func TestStmt_Exec(t *testing.T) {
	drv, cleanup := newDriver(t)
	defer cleanup()

	conn, err := drv.Open("test.db")
	require.NoError(t, err)

	stmt, err := conn.Prepare("CREATE TABLE test (n INT)")
	require.NoError(t, err)

	_, err = conn.Begin()
	require.NoError(t, err)

	_, err = stmt.Exec(nil)
	require.NoError(t, err)

	require.NoError(t, stmt.Close())

	values := []driver.Value{
		int64(1),
	}

	stmt, err = conn.Prepare("INSERT INTO test(n) VALUES(?)")
	require.NoError(t, err)

	result, err := stmt.Exec(values)
	require.NoError(t, err)

	lastInsertID, err := result.LastInsertId()
	require.NoError(t, err)

	assert.Equal(t, lastInsertID, int64(1))

	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)

	assert.Equal(t, rowsAffected, int64(1))

	require.NoError(t, stmt.Close())

	assert.NoError(t, conn.Close())
}

func TestStmt_ExecTooManyParams(t *testing.T) {
	drv, cleanup := newDriver(t)
	defer cleanup()

	conn, err := drv.Open("test.db")
	require.NoError(t, err)

	stmt, err := conn.Prepare("CREATE TABLE test (n INT)")
	require.NoError(t, err)

	_, err = conn.Begin()
	require.NoError(t, err)

	_, err = stmt.Exec(nil)
	require.NoError(t, err)

	require.NoError(t, stmt.Close())

	stmt, err = conn.Prepare("INSERT INTO test(n) VALUES " + strings.Repeat("(?), ", 255) + " (?)")
	require.NoError(t, err)

	values := make([]driver.Value, 256)
	for i := range values {
		values[i] = int64(1)
	}
	_, err = stmt.Exec(values)
	require.Errorf(t, err, "too many parameters (256) max = 255")

	require.NoError(t, stmt.Close())
	assert.NoError(t, conn.Close())
}

func TestStmt_Query(t *testing.T) {
	drv, cleanup := newDriver(t)
	defer cleanup()

	conn, err := drv.Open("test.db")
	require.NoError(t, err)

	stmt, err := conn.Prepare("CREATE TABLE test (n INT)")
	require.NoError(t, err)

	_, err = conn.Begin()
	require.NoError(t, err)

	_, err = stmt.Exec(nil)
	require.NoError(t, err)

	require.NoError(t, stmt.Close())

	stmt, err = conn.Prepare("INSERT INTO test(n) VALUES(-123)")
	require.NoError(t, err)

	_, err = stmt.Exec(nil)
	require.NoError(t, err)

	require.NoError(t, stmt.Close())

	stmt, err = conn.Prepare("SELECT n FROM test")
	require.NoError(t, err)

	rows, err := stmt.Query(nil)
	require.NoError(t, err)

	assert.Equal(t, rows.Columns(), []string{"n"})

	values := make([]driver.Value, 1)
	require.NoError(t, rows.Next(values))

	assert.Equal(t, int64(-123), values[0])

	require.Equal(t, io.EOF, rows.Next(values))

	require.NoError(t, stmt.Close())

	assert.NoError(t, conn.Close())
}

func TestStmt_QueryTooManyParams(t *testing.T) {
	drv, cleanup := newDriver(t)
	defer cleanup()

	conn, err := drv.Open("test.db")
	require.NoError(t, err)

	stmt, err := conn.Prepare("CREATE TABLE test (n INT)")
	require.NoError(t, err)

	_, err = conn.Begin()
	require.NoError(t, err)

	_, err = stmt.Exec(nil)
	require.NoError(t, err)

	require.NoError(t, stmt.Close())

	stmt, err = conn.Prepare("SELECT n FROM test WHERE n IN (" + strings.Repeat("?, ", 255) + " ?)")
	require.NoError(t, err)

	values := make([]driver.Value, 256)
	for i := range values {
		values[i] = int64(1)
	}
	_, err = stmt.Query(values)
	require.Errorf(t, err, "too many parameters (256) max = 255")

	require.NoError(t, stmt.Close())
	assert.NoError(t, conn.Close())
}

func TestConn_QueryParams(t *testing.T) {
	drv, cleanup := newDriver(t)
	defer cleanup()

	conn, err := drv.Open("test.db")
	require.NoError(t, err)

	_, err = conn.Begin()
	require.NoError(t, err)

	execer := conn.(driver.Execer)

	_, err = execer.Exec("CREATE TABLE test (n INT, t TEXT)", nil)
	require.NoError(t, err)

	_, err = execer.Exec(`
INSERT INTO test (n,t) VALUES (1,'a');
INSERT INTO test (n,t) VALUES (2,'a');
INSERT INTO test (n,t) VALUES (2,'b');
INSERT INTO test (n,t) VALUES (3,'b');
`,
		nil)
	require.NoError(t, err)

	values := []driver.Value{
		int64(1),
		"a",
	}

	queryer := conn.(driver.Queryer)

	rows, err := queryer.Query("SELECT n, t FROM test WHERE n > ? AND t = ?", values)
	require.NoError(t, err)

	assert.Equal(t, rows.Columns()[0], "n")

	values = make([]driver.Value, 2)
	require.NoError(t, rows.Next(values))

	assert.Equal(t, int64(2), values[0])
	assert.Equal(t, "a", values[1])

	require.Equal(t, io.EOF, rows.Next(values))

	assert.NoError(t, conn.Close())
}

func TestConn_QueryTooManyParams(t *testing.T) {
	drv, cleanup := newDriver(t)
	defer cleanup()

	conn, err := drv.Open("test.db")
	require.NoError(t, err)

	_, err = conn.Begin()
	require.NoError(t, err)

	execer := conn.(driver.Execer)

	_, err = execer.Exec("CREATE TABLE test (n INT)", nil)
	require.NoError(t, err)

	values := make([]driver.Value, 256)
	for i := range values {
		values[i] = int64(1)
	}
	queryer := conn.(driver.Queryer)
	_, err = queryer.Query("SELECT n FROM test WHERE n IN ("+strings.Repeat("?, ", 255)+" ?)", values)
	require.Errorf(t, err, "too many parameters (256) max = 255")

	assert.NoError(t, conn.Close())
}

func TestConn_ExecTooManyParams(t *testing.T) {
	drv, cleanup := newDriver(t)
	defer cleanup()

	conn, err := drv.Open("test.db")
	require.NoError(t, err)

	_, err = conn.Begin()
	require.NoError(t, err)

	execer := conn.(driver.Execer)

	_, err = execer.Exec("CREATE TABLE test (n INT)", nil)
	require.NoError(t, err)

	values := make([]driver.Value, 256)
	for i := range values {
		values[i] = int64(1)
	}

	_, err = execer.Exec("INSERT INTO test(n) VALUES "+strings.Repeat("(?), ", 255)+" (?)", values)
	require.Errorf(t, err, "too many parameters (256) max = 255")

	assert.NoError(t, conn.Close())
}

func Test_ColumnTypesEmpty(t *testing.T) {
	t.Skip("this currently fails if the result set is empty, is dqlite skipping the header if empty set?")
	drv, cleanup := newDriver(t)
	defer cleanup()

	conn, err := drv.Open("test.db")
	require.NoError(t, err)

	stmt, err := conn.Prepare("CREATE TABLE test (n INT)")
	require.NoError(t, err)

	_, err = conn.Begin()
	require.NoError(t, err)

	_, err = stmt.Exec(nil)
	require.NoError(t, err)

	require.NoError(t, stmt.Close())

	stmt, err = conn.Prepare("SELECT n FROM test")
	require.NoError(t, err)

	rows, err := stmt.Query(nil)
	require.NoError(t, err)

	require.NoError(t, err)
	rowTypes, ok := rows.(driver.RowsColumnTypeDatabaseTypeName)
	require.True(t, ok)

	typeName := rowTypes.ColumnTypeDatabaseTypeName(0)
	assert.Equal(t, "INTEGER", typeName)

	require.NoError(t, stmt.Close())

	assert.NoError(t, conn.Close())
}

func Test_ColumnTypesExists(t *testing.T) {
	drv, cleanup := newDriver(t)
	defer cleanup()

	conn, err := drv.Open("test.db")
	require.NoError(t, err)

	stmt, err := conn.Prepare("CREATE TABLE test (n INT)")
	require.NoError(t, err)

	_, err = conn.Begin()
	require.NoError(t, err)

	_, err = stmt.Exec(nil)
	require.NoError(t, err)

	require.NoError(t, stmt.Close())

	stmt, err = conn.Prepare("INSERT INTO test(n) VALUES(-123)")
	require.NoError(t, err)

	_, err = stmt.Exec(nil)
	require.NoError(t, err)

	stmt, err = conn.Prepare("SELECT n FROM test")
	require.NoError(t, err)

	rows, err := stmt.Query(nil)
	require.NoError(t, err)

	require.NoError(t, err)
	rowTypes, ok := rows.(driver.RowsColumnTypeDatabaseTypeName)
	require.True(t, ok)

	typeName := rowTypes.ColumnTypeDatabaseTypeName(0)
	assert.Equal(t, "INTEGER", typeName)

	require.NoError(t, stmt.Close())
	assert.NoError(t, conn.Close())
}

// ensure column types data is available
// even after the last row of the query
func Test_ColumnTypesEnd(t *testing.T) {
	drv, cleanup := newDriver(t)
	defer cleanup()

	conn, err := drv.Open("test.db")
	require.NoError(t, err)

	stmt, err := conn.Prepare("CREATE TABLE test (n INT)")
	require.NoError(t, err)

	_, err = conn.Begin()
	require.NoError(t, err)

	_, err = stmt.Exec(nil)
	require.NoError(t, err)

	require.NoError(t, stmt.Close())

	stmt, err = conn.Prepare("INSERT INTO test(n) VALUES(-123)")
	require.NoError(t, err)

	_, err = stmt.Exec(nil)
	require.NoError(t, err)

	stmt, err = conn.Prepare("SELECT n FROM test")
	require.NoError(t, err)

	rows, err := stmt.Query(nil)
	require.NoError(t, err)

	require.NoError(t, err)
	rowTypes, ok := rows.(driver.RowsColumnTypeDatabaseTypeName)
	require.True(t, ok)

	typeName := rowTypes.ColumnTypeDatabaseTypeName(0)
	assert.Equal(t, "INTEGER", typeName)

	values := make([]driver.Value, 1)
	require.NoError(t, rows.Next(values))

	assert.Equal(t, int64(-123), values[0])

	require.Equal(t, io.EOF, rows.Next(values))

	// despite EOF we should have types cached
	typeName = rowTypes.ColumnTypeDatabaseTypeName(0)
	assert.Equal(t, "INTEGER", typeName)

	require.NoError(t, stmt.Close())
	assert.NoError(t, conn.Close())
}

func newDriver(t *testing.T) (*dqlitedriver.Driver, func()) {
	t.Helper()

	_, cleanup := newNode(t)

	store := newStore(t, "@1")

	log := logging.Test(t)

	driver, err := dqlitedriver.New(store, dqlitedriver.WithLogFunc(log))
	require.NoError(t, err)

	return driver, cleanup
}

// Create a new in-memory server store populated with the given addresses.
func newStore(t *testing.T, address string) client.NodeStore {
	t.Helper()

	store := client.NewInmemNodeStore()
	server := client.NodeInfo{Address: address}
	require.NoError(t, store.Set(context.Background(), []client.NodeInfo{server}))

	return store
}

func newNode(t *testing.T) (*dqlite.Node, func()) {
	t.Helper()
	dir, dirCleanup := newDir(t)

	server, err := dqlite.New(uint64(1), "@1", dir, dqlite.WithBindAddress("@1"))
	require.NoError(t, err)

	err = server.Start()
	require.NoError(t, err)

	cleanup := func() {
		require.NoError(t, server.Close())
		dirCleanup()
	}

	return server, cleanup
}

// Return a new temporary directory.
func newDir(t *testing.T) (string, func()) {
	t.Helper()

	dir, err := ioutil.TempDir("", "dqlite-replication-test-")
	assert.NoError(t, err)

	cleanup := func() {
		_, err := os.Stat(dir)
		if err != nil {
			assert.True(t, os.IsNotExist(err))
		} else {
			assert.NoError(t, os.RemoveAll(dir))
		}
	}

	return dir, cleanup
}
