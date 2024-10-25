package app_test

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/canonical/go-dqlite"
	"github.com/canonical/go-dqlite/app"
	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/go-dqlite/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Run a cluster using App and then recover it.
func TestRecovery(t *testing.T) {
	dirs := make([]string, 3)
	for i := range dirs {
		dir, err := ioutil.TempDir("", "dqlite-test-")
		require.NoError(t, err)
		dirs[i] = dir
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	apps := make([]*app.App, 3)
	addrs := []string{"@1", "@2", "@3"}
	for i := range apps {
		app, err := app.New(dirs[i], app.WithAddress(addrs[i]), app.WithCluster(addrs[:i]), app.WithLogFunc(makeLogFunc(t, i)))
		require.NoError(t, err)
		err = app.Ready(ctx)
		require.NoError(t, err)
		apps[i] = app
	}

	// run a transaction
	db, err := apps[0].Open(ctx, "test.db")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `CREATE TABLE foo (n INTEGER); INSERT INTO foo VALUES (42)`)
	require.NoError(t, err)
	assert.NoError(t, err)

	// stop the third node so it won't receive data
	err = apps[2].Close()
	require.NoError(t, err)

	// another transaction
	db, err = apps[0].Open(ctx, "test.db")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO foo VALUES (17)`)
	require.NoError(t, err)
	err = db.Close()
	require.NoError(t, err)

	// close the leader
	err = apps[0].Close()
	require.NoError(t, err)

	// can't commit a change
	shortCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, err = apps[1].Open(shortCtx, "test.db")
	require.Error(t, err)

	// take down the final node for recovery
	err = apps[1].Close()
	require.NoError(t, err)

	infos := make([]dqlite.LastEntryInfo, 3)
	for i, dir := range dirs {
		info, err := dqlite.ReadLastEntryInfo(dir)
		require.NoError(t, err)
		infos[i] = info
	}
	require.Equal(t, dqlite.LastEntryInfo{Term: 1, Index: 8}, infos[0])
	require.Equal(t, dqlite.LastEntryInfo{Term: 1, Index: 8}, infos[1])
	require.Equal(t, dqlite.LastEntryInfo{Term: 1, Index: 7}, infos[2])
	require.True(t, infos[2].Before(infos[1]))

	cluster := []client.NodeInfo{
		{Address: "@2", ID: dqlite.GenerateID("@2"), Role: client.Voter},
		{Address: "@3", ID: dqlite.GenerateID("@3"), Role: client.Voter},
	}
	kern, err := app.PrepareRecovery(dirs[1], addrs[1], cluster)
	require.NoError(t, err)

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err = enc.Encode(kern)
	require.NoError(t, err)
	decodedKern := &app.RecoveryKernel{}
	dec := gob.NewDecoder(&buf)
	err = dec.Decode(decodedKern)
	require.NoError(t, err)
	require.Equal(t, kern, decodedKern)

	err = kern.Propagate(dirs[2], addrs[2])
	require.NoError(t, err)

	for i := 1; i < 3; i++ {
		// No app.WithCluster this time since recovery has written the node store.
		app, err := app.New(dirs[i], app.WithAddress(addrs[i]), app.WithLogFunc(makeLogFunc(t, i)))
		require.NoError(t, err)
		apps[i] = app
		// Don't call app.Ready yet, since it's not possible to have a leader after
		// only app[1] has been started.
	}
	for i := 1; i < 3; i++ {
		err = apps[i].Ready(ctx)
		require.NoError(t, err)
	}

	// Only two nodes in the configuration.
	cli, err := apps[1].Client(ctx)
	require.NoError(t, err)
	foundCluster, err := cli.Cluster(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, cluster, foundCluster)

	// The data from before is still there.
	db, err = apps[1].Open(ctx, "test.db")
	require.NoError(t, err)
	rows, err := db.QueryContext(ctx, `SELECT n FROM foo`)
	require.NoError(t, err)
	defer rows.Close()
	var values []int
	for rows.Next() {
		var n int
		err = rows.Scan(&n)
		require.NoError(t, err)
		values = append(values, n)
	}
	require.NoError(t, rows.Err())
	assert.ElementsMatch(t, []int{42, 17}, values)

	// We can commmit a new transaction.
	_, err = db.ExecContext(ctx, `INSERT INTO foo VALUES (99)`)
	require.NoError(t, err)

	err = apps[2].Close()
	require.NoError(t, err)
	err = apps[1].Close()
	require.NoError(t, err)

	for i := 1; i < 3; i++ {
		info, err := dqlite.ReadLastEntryInfo(dirs[i])
		require.NoError(t, err)
		infos[i] = info
	}
	// Term 2 started with the election of a new leader after recovery.
	// Beyond the previous highest index we have three new entries: the
	// new configuration forced by recovery, a barrier run by the leader
	// to discover the commit index before querying, and the final transaction.
	require.Equal(t, infos[1:], []dqlite.LastEntryInfo{
		{Term: 2, Index: 11},
		{Term: 2, Index: 11},
	})
}

func makeLogFunc(t *testing.T, index int) logging.Func {
	return func(l client.LogLevel, format string, a ...interface{}) {
		format = fmt.Sprintf("%s - %d: %s: %s\n", time.Now().Format("15:04:01.000"), index, l.String(), format)
		t.Logf(format, a...)
	}
}
