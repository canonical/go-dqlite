package app_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/canonical/go-dqlite/app"
	"github.com/canonical/go-dqlite/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Create a pristine bootstrap node with default value.
func TestNew_PristineDefault(t *testing.T) {
	_, cleanup := newApp(t)
	defer cleanup()
}

// Create a pristine joining node.
func TestNew_PristineJoiner(t *testing.T) {
	addr1 := "127.0.0.1:9001"
	addr2 := "127.0.0.1:9002"

	app1, cleanup := newApp(t, app.WithAddress(addr1))
	defer cleanup()

	app2, cleanup := newApp(t, app.WithAddress(addr2), app.WithCluster([]string{addr1}))
	defer cleanup()

	require.NoError(t, app2.Ready(context.Background()))

	// The joining node to appear in the cluster list.
	cli, err := app1.Leader(context.Background())
	require.NoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	require.NoError(t, err)
	assert.Equal(t, addr1, cluster[0].Address)
	assert.Equal(t, addr2, cluster[1].Address)

	// Initially the node joins as spare.
	assert.Equal(t, client.Voter, cluster[0].Role)
	assert.Equal(t, client.Spare, cluster[1].Role)
}

// Restart a node that had previously joined the cluster successfully.
func TestNew_JoinerRestart(t *testing.T) {
	addr1 := "127.0.0.1:9001"
	addr2 := "127.0.0.1:9002"

	app1, cleanup := newApp(t, app.WithAddress(addr1))
	defer cleanup()

	require.NoError(t, app1.Ready(context.Background()))

	dir2, cleanup := newDir(t)
	defer cleanup()

	app2, cleanup := newAppWithDir(t, dir2, app.WithAddress(addr2), app.WithCluster([]string{addr1}))
	require.NoError(t, app2.Ready(context.Background()))
	cleanup()

	app2, cleanup = newAppWithDir(t, dir2, app.WithAddress(addr2))
	defer cleanup()

	require.NoError(t, app2.Ready(context.Background()))
}

// The second joiner promotes itself and also the first joiner.
func TestNew_SecondJoiner(t *testing.T) {
	addr1 := "127.0.0.1:9001"
	addr2 := "127.0.0.1:9002"
	addr3 := "127.0.0.1:9003"

	app1, cleanup := newApp(t, app.WithAddress(addr1))
	defer cleanup()

	app2, cleanup := newApp(t, app.WithAddress(addr2), app.WithCluster([]string{addr1}))
	defer cleanup()

	require.NoError(t, app2.Ready(context.Background()))

	app3, cleanup := newApp(t, app.WithAddress(addr3), app.WithCluster([]string{addr1}))
	defer cleanup()

	require.NoError(t, app3.Ready(context.Background()))

	cli, err := app1.Leader(context.Background())
	require.NoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	require.NoError(t, err)

	assert.Equal(t, addr1, cluster[0].Address)
	assert.Equal(t, addr2, cluster[1].Address)
	assert.Equal(t, addr3, cluster[2].Address)

	assert.Equal(t, client.Voter, cluster[0].Role)
	assert.Equal(t, client.Voter, cluster[1].Role)
	assert.Equal(t, client.Voter, cluster[2].Role)
}

// The third joiner gets the stand-by role.
func TestNew_ThirdJoiner(t *testing.T) {
	apps := []*app.App{}

	for i := 0; i < 4; i++ {
		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{app.WithAddress(addr)}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, cleanup := newApp(t, options...)
		defer cleanup()

		require.NoError(t, app.Ready(context.Background()))

		apps = append(apps, app)

	}

	cli, err := apps[0].Leader(context.Background())
	require.NoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	require.NoError(t, err)

	assert.Equal(t, client.Voter, cluster[0].Role)
	assert.Equal(t, client.Voter, cluster[1].Role)
	assert.Equal(t, client.Voter, cluster[2].Role)
	assert.Equal(t, client.StandBy, cluster[3].Role)
}

// The fourth joiner gets the stand-by role.
func TestNew_FourthJoiner(t *testing.T) {
	apps := []*app.App{}

	for i := 0; i < 5; i++ {
		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{app.WithAddress(addr)}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, cleanup := newApp(t, options...)
		defer cleanup()

		require.NoError(t, app.Ready(context.Background()))

		apps = append(apps, app)

	}

	cli, err := apps[0].Leader(context.Background())
	require.NoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	require.NoError(t, err)

	assert.Equal(t, client.Voter, cluster[0].Role)
	assert.Equal(t, client.Voter, cluster[1].Role)
	assert.Equal(t, client.Voter, cluster[2].Role)
	assert.Equal(t, client.StandBy, cluster[3].Role)
	assert.Equal(t, client.StandBy, cluster[4].Role)
}

// The fifth joiner gets the stand-by role.
func TestNew_FifthJoiner(t *testing.T) {
	apps := []*app.App{}

	for i := 0; i < 6; i++ {
		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{app.WithAddress(addr)}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, cleanup := newApp(t, options...)
		defer cleanup()

		require.NoError(t, app.Ready(context.Background()))

		apps = append(apps, app)

	}

	cli, err := apps[0].Leader(context.Background())
	require.NoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	require.NoError(t, err)

	assert.Equal(t, client.Voter, cluster[0].Role)
	assert.Equal(t, client.Voter, cluster[1].Role)
	assert.Equal(t, client.Voter, cluster[2].Role)
	assert.Equal(t, client.StandBy, cluster[3].Role)
	assert.Equal(t, client.StandBy, cluster[4].Role)
	assert.Equal(t, client.StandBy, cluster[5].Role)
}

// The sixth joiner gets the spare role.
func TestNew_SixthJoiner(t *testing.T) {
	apps := []*app.App{}

	for i := 0; i < 7; i++ {
		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{app.WithAddress(addr)}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, cleanup := newApp(t, options...)
		defer cleanup()

		require.NoError(t, app.Ready(context.Background()))

		apps = append(apps, app)

	}

	cli, err := apps[0].Leader(context.Background())
	require.NoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	require.NoError(t, err)

	assert.Equal(t, client.Voter, cluster[0].Role)
	assert.Equal(t, client.Voter, cluster[1].Role)
	assert.Equal(t, client.Voter, cluster[2].Role)
	assert.Equal(t, client.StandBy, cluster[3].Role)
	assert.Equal(t, client.StandBy, cluster[4].Role)
	assert.Equal(t, client.StandBy, cluster[5].Role)
	assert.Equal(t, client.Spare, cluster[6].Role)
}

// Transfer voting rights to another online node.
func TestHandover_Voter(t *testing.T) {
	n := 4
	apps := make([]*app.App, n)

	for i := 0; i < n; i++ {
		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{app.WithAddress(addr)}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, cleanup := newApp(t, options...)
		defer cleanup()

		require.NoError(t, app.Ready(context.Background()))

		apps[i] = app
	}

	cli, err := apps[0].Leader(context.Background())
	require.NoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	require.NoError(t, err)

	assert.Equal(t, client.Voter, cluster[0].Role)
	assert.Equal(t, client.Voter, cluster[1].Role)
	assert.Equal(t, client.Voter, cluster[2].Role)
	assert.Equal(t, client.StandBy, cluster[3].Role)

	require.NoError(t, apps[2].Handover(context.Background()))

	cluster, err = cli.Cluster(context.Background())
	require.NoError(t, err)

	assert.Equal(t, client.Voter, cluster[0].Role)
	assert.Equal(t, client.Voter, cluster[1].Role)
	assert.Equal(t, client.Spare, cluster[2].Role)
	assert.Equal(t, client.Voter, cluster[3].Role)
}

// In a two-node cluster only one of them is a voter. When Handover() is called
// on the voter, the role and leadership are transfered.
func TestHandover_TwoNodes(t *testing.T) {
	n := 2
	apps := make([]*app.App, n)

	for i := 0; i < n; i++ {
		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{app.WithAddress(addr)}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, cleanup := newApp(t, options...)
		defer cleanup()

		require.NoError(t, app.Ready(context.Background()))

		apps[i] = app
	}

	err := apps[0].Handover(context.Background())
	require.NoError(t, err)

	cli, err := apps[1].Leader(context.Background())
	require.NoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	require.NoError(t, err)

	assert.Equal(t, client.Spare, cluster[0].Role)
	assert.Equal(t, client.Voter, cluster[1].Role)
}

// Transfer voting rights to another online node. Failure domains are taken
// into account.
func TestHandover_VoterHonorFailureDomain(t *testing.T) {
	n := 6
	apps := make([]*app.App, n)

	for i := 0; i < n; i++ {
		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{
			app.WithAddress(addr),
			app.WithFailureDomain(uint64(i % 3)),
		}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, cleanup := newApp(t, options...)
		defer cleanup()

		require.NoError(t, app.Ready(context.Background()))

		apps[i] = app
	}

	cli, err := apps[0].Leader(context.Background())
	require.NoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	require.NoError(t, err)

	require.NoError(t, apps[2].Handover(context.Background()))

	cluster, err = cli.Cluster(context.Background())
	require.NoError(t, err)

	assert.Equal(t, client.Voter, cluster[0].Role)
	assert.Equal(t, client.Voter, cluster[1].Role)
	assert.Equal(t, client.Spare, cluster[2].Role)
	assert.Equal(t, client.StandBy, cluster[3].Role)
	assert.Equal(t, client.StandBy, cluster[4].Role)
	assert.Equal(t, client.Voter, cluster[5].Role)
}

// Handover with a sinle node.
func TestHandover_SingleNode(t *testing.T) {
	dir, cleanup := newDir(t)
	defer cleanup()

	app, err := app.New(dir, app.WithAddress("127.0.0.1:9001"))
	require.NoError(t, err)

	require.NoError(t, app.Ready(context.Background()))

	require.NoError(t, app.Handover(context.Background()))
	require.NoError(t, app.Close())
}

// Exercise a sequential graceful shutdown of a 3-node cluster.
func TestHandover_GracefulShutdown(t *testing.T) {
	n := 3
	apps := make([]*app.App, n)

	for i := 0; i < n; i++ {
		dir, cleanup := newDir(t)
		defer cleanup()

		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{
			app.WithAddress(addr),
		}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, err := app.New(dir, options...)
		require.NoError(t, err)

		require.NoError(t, app.Ready(context.Background()))

		apps[i] = app
	}

	db, err := sql.Open(apps[0].Driver(), "test.db")
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE test (n INT)")
	require.NoError(t, err)

	require.NoError(t, db.Close())

	require.NoError(t, apps[0].Handover(context.Background()))
	require.NoError(t, apps[0].Close())

	require.NoError(t, apps[1].Handover(context.Background()))
	require.NoError(t, apps[1].Close())

	require.NoError(t, apps[2].Handover(context.Background()))
	require.NoError(t, apps[2].Close())
}

// Transfer the stand-by role to another online node.
func TestHandover_StandBy(t *testing.T) {
	n := 7
	apps := make([]*app.App, n)

	for i := 0; i < n; i++ {
		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{app.WithAddress(addr)}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, cleanup := newApp(t, options...)
		defer cleanup()

		require.NoError(t, app.Ready(context.Background()))

		apps[i] = app
	}

	cli, err := apps[0].Leader(context.Background())
	require.NoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	require.NoError(t, err)

	assert.Equal(t, client.Voter, cluster[0].Role)
	assert.Equal(t, client.Voter, cluster[1].Role)
	assert.Equal(t, client.Voter, cluster[2].Role)
	assert.Equal(t, client.StandBy, cluster[3].Role)
	assert.Equal(t, client.StandBy, cluster[4].Role)
	assert.Equal(t, client.StandBy, cluster[5].Role)
	assert.Equal(t, client.Spare, cluster[6].Role)

	require.NoError(t, apps[4].Handover(context.Background()))

	cluster, err = cli.Cluster(context.Background())
	require.NoError(t, err)

	assert.Equal(t, client.Voter, cluster[0].Role)
	assert.Equal(t, client.Voter, cluster[1].Role)
	assert.Equal(t, client.Voter, cluster[2].Role)
	assert.Equal(t, client.StandBy, cluster[3].Role)
	assert.Equal(t, client.Spare, cluster[4].Role)
	assert.Equal(t, client.StandBy, cluster[5].Role)
	assert.Equal(t, client.StandBy, cluster[6].Role)
}

// Transfer leadership and voting rights to another node.
func TestHandover_TransferLeadership(t *testing.T) {
	n := 4
	apps := make([]*app.App, n)

	for i := 0; i < n; i++ {
		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{app.WithAddress(addr)}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, cleanup := newApp(t, options...)
		defer cleanup()

		require.NoError(t, app.Ready(context.Background()))

		apps[i] = app
	}

	cli, err := apps[0].Leader(context.Background())
	require.NoError(t, err)
	defer cli.Close()

	leader, err := cli.Leader(context.Background())
	require.NoError(t, err)

	require.NotNil(t, leader)
	require.Equal(t, apps[0].ID(), leader.ID)
	require.NoError(t, apps[0].Handover(context.Background()))

	cli, err = apps[0].Leader(context.Background())
	require.NoError(t, err)
	defer cli.Close()

	leader, err = cli.Leader(context.Background())
	require.NoError(t, err)

	assert.NotEqual(t, apps[0].ID(), leader.ID)

	cluster, err := cli.Cluster(context.Background())
	require.NoError(t, err)

	assert.Equal(t, client.Spare, cluster[0].Role)
	assert.Equal(t, client.Voter, cluster[1].Role)
	assert.Equal(t, client.Voter, cluster[2].Role)
	assert.Equal(t, client.Voter, cluster[3].Role)
}

// If a voter goes offline, another node takes its place.
func TestRolesAdjustment_ReplaceVoter(t *testing.T) {
	n := 4
	apps := make([]*app.App, n)
	cleanups := make([]func(), n)

	for i := 0; i < n; i++ {
		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{
			app.WithAddress(addr),
			app.WithRolesAdjustmentFrequency(500 * time.Millisecond),
		}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, cleanup := newApp(t, options...)

		require.NoError(t, app.Ready(context.Background()))

		apps[i] = app
		cleanups[i] = cleanup
	}

	defer cleanups[0]()
	defer cleanups[1]()
	defer cleanups[3]()

	// A voter goes offline.
	cleanups[2]()

	time.Sleep(2 * time.Second)

	cli, err := apps[0].Leader(context.Background())
	require.NoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	require.NoError(t, err)

	assert.Equal(t, client.Voter, cluster[0].Role)
	assert.Equal(t, client.Voter, cluster[1].Role)
	assert.Equal(t, client.Spare, cluster[2].Role)
	assert.Equal(t, client.Voter, cluster[3].Role)
}

// If a voter goes offline, another node takes its place. If possible, pick a
// voter from a failure domain which differs from the one of the two other
// voters.
func TestRolesAdjustment_ReplaceVoterHonorFailureDomain(t *testing.T) {
	n := 6
	apps := make([]*app.App, n)
	cleanups := make([]func(), n)

	for i := 0; i < n; i++ {
		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{
			app.WithAddress(addr),
			app.WithRolesAdjustmentFrequency(500 * time.Millisecond),
			app.WithFailureDomain(uint64(i % 3)),
		}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, cleanup := newApp(t, options...)

		require.NoError(t, app.Ready(context.Background()))

		apps[i] = app
		cleanups[i] = cleanup
	}

	defer cleanups[0]()
	defer cleanups[1]()
	defer cleanups[3]()
	defer cleanups[4]()
	defer cleanups[5]()

	// A voter in failure domain 2 goes offline.
	cleanups[2]()

	time.Sleep(2 * time.Second)

	cli, err := apps[0].Leader(context.Background())
	require.NoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	require.NoError(t, err)

	// The replacement was picked in the same failure domain.
	assert.Equal(t, client.Voter, cluster[0].Role)
	assert.Equal(t, client.Voter, cluster[1].Role)
	assert.Equal(t, client.Spare, cluster[2].Role)
	assert.Equal(t, client.StandBy, cluster[3].Role)
	assert.Equal(t, client.StandBy, cluster[4].Role)
	assert.Equal(t, client.Voter, cluster[5].Role)
}

// If a voter goes offline, another node takes its place. Preference will be
// given to candidates with lower weights.
func TestRolesAdjustment_ReplaceVoterHonorWeight(t *testing.T) {
	n := 6
	apps := make([]*app.App, n)
	cleanups := make([]func(), n)

	for i := 0; i < n; i++ {
		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{
			app.WithAddress(addr),
			app.WithRolesAdjustmentFrequency(500 * time.Millisecond),
		}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, cleanup := newApp(t, options...)

		require.NoError(t, app.Ready(context.Background()))

		apps[i] = app
		cleanups[i] = cleanup
	}

	defer cleanups[0]()
	defer cleanups[1]()
	defer cleanups[3]()
	defer cleanups[4]()
	defer cleanups[5]()

	// A voter in failure domain 2 goes offline.
	cleanups[2]()

	cli, err := apps[3].Client(context.Background())
	require.NoError(t, err)
	require.NoError(t, cli.Weight(context.Background(), uint64(15)))
	defer cli.Close()

	cli, err = apps[4].Client(context.Background())
	require.NoError(t, err)
	require.NoError(t, cli.Weight(context.Background(), uint64(5)))
	defer cli.Close()

	cli, err = apps[5].Client(context.Background())
	require.NoError(t, err)
	require.NoError(t, cli.Weight(context.Background(), uint64(10)))
	defer cli.Close()

	time.Sleep(2 * time.Second)

	cli, err = apps[0].Leader(context.Background())
	require.NoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	require.NoError(t, err)

	// The stand-by with the lowest weight was picked.
	assert.Equal(t, client.Voter, cluster[0].Role)
	assert.Equal(t, client.Voter, cluster[1].Role)
	assert.Equal(t, client.Spare, cluster[2].Role)
	assert.Equal(t, client.StandBy, cluster[3].Role)
	assert.Equal(t, client.Voter, cluster[4].Role)
	assert.Equal(t, client.StandBy, cluster[5].Role)
}

// If a voter goes offline, but no another node can its place, then nothing
// chagnes.
func TestRolesAdjustment_CantReplaceVoter(t *testing.T) {
	n := 4
	apps := make([]*app.App, n)
	cleanups := make([]func(), n)

	for i := 0; i < n; i++ {
		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{
			app.WithAddress(addr),
			app.WithRolesAdjustmentFrequency(500 * time.Millisecond),
		}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, cleanup := newApp(t, options...)

		require.NoError(t, app.Ready(context.Background()))

		apps[i] = app
		cleanups[i] = cleanup
	}

	defer cleanups[0]()
	defer cleanups[1]()

	// A voter and a spare go offline.
	cleanups[3]()
	cleanups[2]()

	time.Sleep(2 * time.Second)

	cli, err := apps[0].Leader(context.Background())
	require.NoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	require.NoError(t, err)

	assert.Equal(t, client.Voter, cluster[0].Role)
	assert.Equal(t, client.Voter, cluster[1].Role)
	assert.Equal(t, client.Voter, cluster[2].Role)
	assert.Equal(t, client.StandBy, cluster[3].Role)
}

// If a stand-by goes offline, another node takes its place.
func TestRolesAdjustment_ReplaceStandBy(t *testing.T) {
	n := 7
	apps := make([]*app.App, n)
	cleanups := make([]func(), n)

	for i := 0; i < n; i++ {
		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{
			app.WithAddress(addr),
			app.WithRolesAdjustmentFrequency(500 * time.Millisecond),
		}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, cleanup := newApp(t, options...)

		require.NoError(t, app.Ready(context.Background()))

		apps[i] = app
		cleanups[i] = cleanup
	}

	defer cleanups[0]()
	defer cleanups[1]()
	defer cleanups[2]()
	defer cleanups[3]()
	defer cleanups[5]()
	defer cleanups[6]()

	// A stand-by goes offline.
	cleanups[4]()

	time.Sleep(2 * time.Second)

	cli, err := apps[0].Leader(context.Background())
	require.NoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	require.NoError(t, err)

	assert.Equal(t, client.Voter, cluster[0].Role)
	assert.Equal(t, client.Voter, cluster[1].Role)
	assert.Equal(t, client.Voter, cluster[2].Role)
	assert.Equal(t, client.StandBy, cluster[3].Role)
	assert.Equal(t, client.Spare, cluster[4].Role)
	assert.Equal(t, client.StandBy, cluster[5].Role)
	assert.Equal(t, client.StandBy, cluster[6].Role)
}

// If a stand-by goes offline, another node takes its place. If possible, pick
// a stand-by from a failure domain which differs from the one of the two other
// stand-bys.
func TestRolesAdjustment_ReplaceStandByHonorFailureDomains(t *testing.T) {
	n := 9
	apps := make([]*app.App, n)
	cleanups := make([]func(), n)

	for i := 0; i < n; i++ {
		addr := fmt.Sprintf("127.0.0.1:900%d", i+1)
		options := []app.Option{
			app.WithAddress(addr),
			app.WithRolesAdjustmentFrequency(500 * time.Millisecond),
			app.WithFailureDomain(uint64(i % 3)),
		}
		if i > 0 {
			options = append(options, app.WithCluster([]string{"127.0.0.1:9001"}))
		}

		app, cleanup := newApp(t, options...)

		require.NoError(t, app.Ready(context.Background()))

		apps[i] = app
		cleanups[i] = cleanup
	}

	defer cleanups[0]()
	defer cleanups[1]()
	defer cleanups[2]()
	defer cleanups[3]()
	defer cleanups[5]()
	defer cleanups[6]()
	defer cleanups[7]()
	defer cleanups[8]()

	// A stand-by from failure domain 1 goes offline.
	cleanups[4]()

	time.Sleep(2 * time.Second)

	cli, err := apps[0].Leader(context.Background())
	require.NoError(t, err)
	defer cli.Close()

	cluster, err := cli.Cluster(context.Background())
	require.NoError(t, err)

	// The replacement was picked in the same failure domain.
	assert.Equal(t, client.Voter, cluster[0].Role)
	assert.Equal(t, client.Voter, cluster[1].Role)
	assert.Equal(t, client.Voter, cluster[2].Role)
	assert.Equal(t, client.StandBy, cluster[3].Role)
	assert.Equal(t, client.Spare, cluster[4].Role)
	assert.Equal(t, client.StandBy, cluster[5].Role)
	assert.Equal(t, client.Spare, cluster[6].Role)
	assert.Equal(t, client.StandBy, cluster[7].Role)
	assert.Equal(t, client.Spare, cluster[8].Role)
}

// Open a database on a fresh one-node cluster.
func TestOpen(t *testing.T) {
	app, cleanup := newApp(t)
	defer cleanup()

	db, err := app.Open(context.Background(), "test")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.ExecContext(context.Background(), "CREATE TABLE foo(n INT)")
	assert.NoError(t, err)
}

// Test client connections dropping uncleanly.
func TestProxy_Error(t *testing.T) {
	cert, pool := loadCert(t)
	dial := client.DialFuncWithTLS(client.DefaultDialFunc, app.SimpleDialTLSConfig(cert, pool))

	_, cleanup := newApp(t, app.WithAddress("127.0.0.1:9000"))
	defer cleanup()

	// Simulate a client which writes the protocol header, then a Leader
	// request and finally drops before reading the response.
	conn, err := dial(context.Background(), "127.0.0.1:9000")
	require.NoError(t, err)

	protocol := make([]byte, 8)
	binary.LittleEndian.PutUint64(protocol, uint64(1))

	n, err := conn.Write(protocol)
	require.NoError(t, err)
	assert.Equal(t, n, 8)

	header := make([]byte, 8)
	binary.LittleEndian.PutUint32(header[0:], 1)
	header[4] = 0
	header[5] = 0
	binary.LittleEndian.PutUint16(header[6:], 0)

	n, err = conn.Write(header)
	require.NoError(t, err)
	assert.Equal(t, n, 8)

	body := make([]byte, 8)
	n, err = conn.Write(body)
	require.NoError(t, err)
	assert.Equal(t, n, 8)

	time.Sleep(100 * time.Millisecond)
	conn.Close()
	time.Sleep(250 * time.Millisecond)
}

// If the given context is cancelled before initial tasks are completed, an
// error is returned.
func TestReady_Cancel(t *testing.T) {
	app, cleanup := newApp(t, app.WithAddress("127.0.0.1:9002"), app.WithCluster([]string{"127.0.0.1:9001"}))
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := app.Ready(ctx)

	assert.Equal(t, ctx.Err(), err)
}

func newApp(t *testing.T, options ...app.Option) (*app.App, func()) {
	t.Helper()

	dir, dirCleanup := newDir(t)

	app, appCleanup := newAppWithDir(t, dir, options...)

	cleanup := func() {
		appCleanup()
		dirCleanup()
	}

	return app, cleanup
}

func newAppWithDir(t *testing.T, dir string, options ...app.Option) (*app.App, func()) {
	t.Helper()

	appIndex++

	index := appIndex
	log := func(l client.LogLevel, format string, a ...interface{}) {
		format = fmt.Sprintf("%s - %d: %s: %s", time.Now().Format("15:04:01.000"), index, l.String(), format)
		t.Logf(format, a...)
	}

	cert, pool := loadCert(t)
	options = append(options, app.WithLogFunc(log), app.WithTLS(app.SimpleTLSConfig(cert, pool)))

	app, err := app.New(dir, options...)
	require.NoError(t, err)

	cleanup := func() {
		require.NoError(t, app.Close())
	}

	return app, cleanup
}

// Loads the test TLS certificates.
func loadCert(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()

	crt := filepath.Join("testdata", "cluster.crt")
	key := filepath.Join("testdata", "cluster.key")

	keypair, err := tls.LoadX509KeyPair(crt, key)
	require.NoError(t, err)

	data, err := ioutil.ReadFile(crt)
	require.NoError(t, err)

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		t.Fatal("bad certificate")
	}

	return keypair, pool
}

var appIndex int

// Return a new temporary directory.
func newDir(t *testing.T) (string, func()) {
	t.Helper()

	dir, err := ioutil.TempDir("", "dqlite-app-test-")
	assert.NoError(t, err)

	cleanup := func() {
		os.RemoveAll(dir)
	}

	return dir, cleanup
}
