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

// Create a new cluster for 3 dqlite drivers exposed over gRPC. Return 3 sql.DB
// instances backed gRPC SQL drivers, each one trying to connect to one of the
// 3 dqlite drivers over gRPC, in a round-robin fashion.

package dqlite_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/CanonicalLtd/go-dqlite"
	"github.com/CanonicalLtd/go-dqlite/internal/logging"
	"github.com/CanonicalLtd/raft-test"
	"github.com/hashicorp/raft"
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

	_, err = tx.Exec("CREATE TABLE test  (n INT)")
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

func TestIntegration_NotLeader(t *testing.T) {
	db, control, cleanup := newDB(t)
	defer cleanup()

	tx, err := db.Begin()
	require.NoError(t, err)

	control.Depose()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err = tx.PrepareContext(ctx, "CREATE TABLE test (n INT)")
	require.Equal(t, driver.ErrBadConn, err)
}

func TestIntegration_LargeQuery_WithTimestamps(t *testing.T) {
	db, _, cleanup := newDB(t)
	defer cleanup()

	tx, err := db.Begin()
	require.NoError(t, err)

	_, err = tx.Exec("CREATE TABLE test (name TEXT, id INTEGER, address TEXT, heartbeat DATETIME)")
	require.NoError(t, err)

	stmt, err := tx.Prepare("INSERT INTO test(name, id, address, heartbeat) VALUES(?,?,?,?)")
	require.NoError(t, err)

	for _, row := range testIntegrationLargeQueryWithTimestampsData {
		_, err = stmt.Exec(row.Name, row.ID, row.Address, row.Heartbeat)
		require.NoError(t, err)
	}

	require.NoError(t, stmt.Close())

	require.NoError(t, tx.Commit())

	tx, err = db.Begin()
	require.NoError(t, err)

	rows, err := tx.Query("SELECT name, id, address, heartbeat FROM test")
	require.NoError(t, err)

	columns, err := rows.Columns()
	require.NoError(t, err)

	assert.Equal(t, []string{"name", "id", "address", "heartbeat"}, columns)

	for i := 0; rows.Next(); i++ {
		var name string
		var id int64
		var address string
		var heartbeat time.Time

		require.NoError(t, rows.Scan(&name, &id, &address, &heartbeat))
	}

	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	require.NoError(t, tx.Rollback())

	require.NoError(t, db.Close())
}

var testIntegrationLargeQueryWithTimestampsData = []struct {
	Name      string
	ID        int64
	Address   string
	Heartbeat string
}{
	{"alpine-34-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"alpine-34-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"alpine-34-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"alpine-35-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"alpine-35-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"alpine-36-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"alpine-36-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"alpine-37-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"alpine-37-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"alpine-38-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"alpine-38-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"alpine-edge-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"alpine-edge-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"archlinux-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"archlinux-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"centos-6-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"centos-6-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"centos-7-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"centos-7-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"debian-10-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"debian-10-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"debian-7-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"debian-7-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"debian-8-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"debian-8-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"debian-9-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"debian-9-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"debian-sid-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"debian-sid-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"fedora-26-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"fedora-26-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"fedora-27-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"fedora-27-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"fedora-28-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"fedora-28-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"gentoo-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"gentoo-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"opensuse-150-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"opensuse-150-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"opensuse-423-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"opensuse-423-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"oracle-6-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"oracle-6-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"oracle-7-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"oracle-7-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"plamo-5x-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"plamo-5x-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"plamo-6x-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"plamo-6x-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"sabayon-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"sabayon-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"ubuntu-core-16-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"ubuntu-core-16-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"ubuntu-1404-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"ubuntu-1404-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"ubuntu-1604-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"ubuntu-1604-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"ubuntu-1710-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"ubuntu-1710-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"ubuntu-1804-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"ubuntu-1804-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"ubuntu-1810-unpriv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
	{"ubuntu-1810-priv", 1, "0.0.0.0", "2018-08-01 04:53:10"},
}

func TestIntegration_LargeQuery_WithLongStrings(t *testing.T) {
	db, _, cleanup := newDB(t)
	defer cleanup()

	tx, err := db.Begin()
	require.NoError(t, err)

	_, err = tx.Exec("CREATE TABLE containers_config (id INTEGER, container_id INTEGER, key TEXT, value TEXT)")
	require.NoError(t, err)

	_, err = tx.Exec(testIntegrationLargeQueryWithLongStringsData)
	require.NoError(t, err)

	require.NoError(t, tx.Commit())

	tx, err = db.Begin()
	require.NoError(t, err)

	rows, err := tx.Query("SELECT container_id, key, value FROM containers_config")
	require.NoError(t, err)

	columns, err := rows.Columns()
	require.NoError(t, err)

	assert.Equal(t, []string{"container_id", "key", "value"}, columns)

	for i := 0; rows.Next(); i++ {
		var id int64
		var key string
		var value string

		require.NoError(t, rows.Scan(&id, &key, &value))
	}

	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	require.NoError(t, tx.Rollback())

	require.NoError(t, db.Close())
}

var testIntegrationLargeQueryWithLongStringsData = `
INSERT INTO containers_config VALUES(463,1,'image.description','Busybox x86_64');
INSERT INTO containers_config VALUES(464,1,'volatile.apply_template','create');
INSERT INTO containers_config VALUES(465,1,'volatile.idmap.next','[{"Isuid":true,"Isgid":false,"Hostid":100000,"Nsid":0,"Maprange":1000000000},{"Isuid":false,"Isgid":true,"Hostid":100000,"Nsid":0,"Maprange":1000000000}]');
INSERT INTO containers_config VALUES(466,1,'volatile.base_image','d5c70f9097d133ba56c424733cd5a30e1e8519a7b850b24e95fec47e10c6e5fa');
INSERT INTO containers_config VALUES(467,1,'volatile.eth0.hwaddr','00:16:3e:ac:4b:17');
INSERT INTO containers_config VALUES(468,1,'volatile.idmap.base','0');
INSERT INTO containers_config VALUES(469,1,'volatile.last_state.idmap','[{"Isuid":true,"Isgid":false,"Hostid":100000,"Nsid":0,"Maprange":1000000000},{"Isuid":false,"Isgid":true,"Hostid":100000,"Nsid":0,"Maprange":1000000000}]');
INSERT INTO containers_config VALUES(470,1,'image.architecture','x86_64');
INSERT INTO containers_config VALUES(471,1,'image.name','busybox-x86_64');
INSERT INTO containers_config VALUES(472,1,'image.os','Busybox');
INSERT INTO containers_config VALUES(473,2,'image.description','Busybox x86_64');
INSERT INTO containers_config VALUES(474,2,'image.os','Busybox');
INSERT INTO containers_config VALUES(475,2,'volatile.apply_template','create');
INSERT INTO containers_config VALUES(476,2,'volatile.base_image','d5c70f9097d133ba56c424733cd5a30e1e8519a7b850b24e95fec47e10c6e5fa');
INSERT INTO containers_config VALUES(477,2,'volatile.eth0.hwaddr','00:16:3e:64:43:68');
INSERT INTO containers_config VALUES(478,2,'volatile.idmap.base','0');
INSERT INTO containers_config VALUES(479,2,'volatile.idmap.next','[{"Isuid":true,"Isgid":false,"Hostid":100000,"Nsid":0,"Maprange":1000000000},{"Isuid":false,"Isgid":true,"Hostid":100000,"Nsid":0,"Maprange":1000000000}]');
INSERT INTO containers_config VALUES(480,2,'image.architecture','x86_64');
INSERT INTO containers_config VALUES(481,2,'volatile.last_state.idmap','[{"Isuid":true,"Isgid":false,"Hostid":100000,"Nsid":0,"Maprange":1000000000},{"Isuid":false,"Isgid":true,"Hostid":100000,"Nsid":0,"Maprange":1000000000}]');
INSERT INTO containers_config VALUES(482,2,'image.name','busybox-x86_64');
INSERT INTO containers_config VALUES(483,3,'volatile.idmap.next','[{"Isuid":true,"Isgid":false,"Hostid":100000,"Nsid":0,"Maprange":1000000000},{"Isuid":false,"Isgid":true,"Hostid":100000,"Nsid":0,"Maprange":1000000000}]');
INSERT INTO containers_config VALUES(484,3,'volatile.last_state.idmap','[{"Isuid":true,"Isgid":false,"Hostid":100000,"Nsid":0,"Maprange":1000000000},{"Isuid":false,"Isgid":true,"Hostid":100000,"Nsid":0,"Maprange":1000000000}]');
INSERT INTO containers_config VALUES(485,3,'image.architecture','x86_64');
INSERT INTO containers_config VALUES(486,3,'image.description','Busybox x86_64');
INSERT INTO containers_config VALUES(487,3,'image.name','busybox-x86_64');
INSERT INTO containers_config VALUES(488,3,'volatile.base_image','d5c70f9097d133ba56c424733cd5a30e1e8519a7b850b24e95fec47e10c6e5fa');
INSERT INTO containers_config VALUES(489,3,'image.os','Busybox');
INSERT INTO containers_config VALUES(490,3,'volatile.eth0.hwaddr','00:16:3e:2b:cd:e6');
INSERT INTO containers_config VALUES(491,3,'volatile.idmap.base','0');
INSERT INTO containers_config VALUES(492,3,'volatile.last_state.power','RUNNING');
INSERT INTO containers_config VALUES(493,4,'image.architecture','x86_64');
INSERT INTO containers_config VALUES(494,4,'image.description','Busybox x86_64');
INSERT INTO containers_config VALUES(495,4,'volatile.base_image','d5c70f9097d133ba56c424733cd5a30e1e8519a7b850b24e95fec47e10c6e5fa');
INSERT INTO containers_config VALUES(496,4,'volatile.idmap.base','0');
INSERT INTO containers_config VALUES(497,4,'volatile.last_state.idmap','[{"Isuid":true,"Isgid":false,"Hostid":100000,"Nsid":0,"Maprange":1000000000},{"Isuid":false,"Isgid":true,"Hostid":100000,"Nsid":0,"Maprange":1000000000}]');
INSERT INTO containers_config VALUES(498,4,'volatile.last_state.power','RUNNING');
INSERT INTO containers_config VALUES(499,4,'image.name','busybox-x86_64');
INSERT INTO containers_config VALUES(500,4,'image.os','Busybox');
INSERT INTO containers_config VALUES(501,4,'volatile.eth0.hwaddr','00:16:3e:f7:28:13');
INSERT INTO containers_config VALUES(502,4,'volatile.idmap.next','[{"Isuid":true,"Isgid":false,"Hostid":100000,"Nsid":0,"Maprange":1000000000},{"Isuid":false,"Isgid":true,"Hostid":100000,"Nsid":0,"Maprange":1000000000}]');
INSERT INTO containers_config VALUES(533,5,'image.description','Busybox x86_64');
INSERT INTO containers_config VALUES(534,5,'volatile.idmap.base','0');
INSERT INTO containers_config VALUES(535,5,'volatile.apply_template','create');
INSERT INTO containers_config VALUES(536,5,'volatile.last_state.idmap','[{"Isuid":true,"Isgid":false,"Hostid":100000,"Nsid":0,"Maprange":1000000000},{"Isuid":false,"Isgid":true,"Hostid":100000,"Nsid":0,"Maprange":1000000000}]');
INSERT INTO containers_config VALUES(537,5,'image.os','Busybox');
INSERT INTO containers_config VALUES(538,5,'image.architecture','x86_64');
INSERT INTO containers_config VALUES(539,5,'image.name','busybox-x86_64');
INSERT INTO containers_config VALUES(540,5,'volatile.base_image','d5c70f9097d133ba56c424733cd5a30e1e8519a7b850b24e95fec47e10c6e5fa');
INSERT INTO containers_config VALUES(541,5,'volatile.idmap.next','[{"Isuid":true,"Isgid":false,"Hostid":100000,"Nsid":0,"Maprange":1000000000},{"Isuid":false,"Isgid":true,"Hostid":100000,"Nsid":0,"Maprange":1000000000}]');
INSERT INTO containers_config VALUES(542,5,'volatile.eth0.hwaddr','00:16:3e:f0:99:38');
`

func newDB(t *testing.T) (*sql.DB, *rafttest.Control, func()) {
	n := 3

	listeners := make([]net.Listener, n)
	servers := make([]dqlite.ServerInfo, n)
	for i := range listeners {
		listeners[i] = newListener(t)
		servers[i].Address = listeners[i].Addr().String()
	}

	control, cleanup := newServers(t, listeners)

	store, err := dqlite.DefaultServerStore(":memory:")
	require.NoError(t, err)

	require.NoError(t, store.Set(context.Background(), servers))

	log := logging.Test(t)
	driver, err := dqlite.NewDriver(store, dqlite.WithLogFunc(log))
	require.NoError(t, err)

	driverName := fmt.Sprintf("dqlite-integration-test-%d", driversCount)
	sql.Register(driverName, driver)

	driversCount++

	db, err := sql.Open(driverName, "test.db")
	require.NoError(t, err)

	return db, control, cleanup
}

func TestIntegration_EmptyTimestamp(t *testing.T) {
	db, _, cleanup := newDB(t)
	defer cleanup()

	tx, err := db.Begin()
	require.NoError(t, err)

	_, err = tx.Exec("CREATE TABLE test (d DATETIME)")
	require.NoError(t, err)

	_, err = tx.Exec("INSERT INTO test(d) VALUES('')")

	rows, err := tx.Query("SELECT d FROM test")
	require.NoError(t, err)

	for rows.Next() {
		var d time.Time

		require.NoError(t, rows.Scan(&d))
		assert.Equal(t, time.Time{}, d)
	}

	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	require.NoError(t, tx.Rollback())

	require.NoError(t, db.Close())
}

func TestIntegration_QueryInterrupt(t *testing.T) {
	db, _, cleanup := newDB(t)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE test (n INT)")
	require.NoError(t, err)

	tx, err := db.Begin()
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

	// This query will yield a multi-response result set, which needs to be
	// cancelled because Rows.Next() will be called only for one row.
	row := tx.QueryRow("SELECT n FROM test")

	var n int64
	err = row.Scan(&n)
	require.NoError(t, err)

	require.NoError(t, tx.Rollback())

	tx, err = db.Begin()
	require.NoError(t, err)

	_, err = tx.Exec("INSERT INTO test(n) VALUES(1)")
	require.NoError(t, err)

	require.NoError(t, tx.Rollback())
}

func newServers(t *testing.T, listeners []net.Listener) (*rafttest.Control, func()) {
	t.Helper()

	n := len(listeners)
	cleanups := make([]func(), 0)

	// Create the dqlite registries and FSMs.
	registries := make([]*dqlite.Registry, n)
	fsms := make([]raft.FSM, n)

	for i := range registries {
		id := strconv.Itoa(i)
		registries[i] = dqlite.NewRegistry(id)
		fsms[i] = dqlite.NewFSM(registries[i])
	}

	// Create the raft cluster using the dqlite FSMs.
	rafts, control := rafttest.Cluster(t, fsms, rafttest.Transport(func(i int) raft.Transport {
		address := raft.ServerAddress(listeners[i].Addr().String())
		_, transport := raft.NewInmemTransport(address)
		return transport
	}))
	control.Elect("0")

	for id := range rafts {
		r := rafts[id]
		i, err := strconv.Atoi(string(id))
		require.NoError(t, err)

		log := logging.Test(t)

		server, err := dqlite.NewServer(
			r, registries[i], listeners[i],
			dqlite.WithServerLogFunc(log))
		require.NoError(t, err)

		cleanups = append(cleanups, func() {
			require.NoError(t, server.Close())
		})

	}

	cleanup := func() {
		control.Close()
		for _, f := range cleanups {
			f()
		}
	}

	return control, cleanup
}

var driversCount = 0
