package app_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
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

	_, cleanup = newApp(t, app.WithAddress(addr2), app.WithCluster([]string{addr1}))
	defer cleanup()

	// Eventually the joining nodes appear in the cluster list.
	cli, err := app1.Leader(context.Background())
	require.NoError(t, err)

	joined := false
	for i := 0; i < 20; i++ {
		time.Sleep(50 * time.Millisecond)
		cluster, err := cli.Cluster(context.Background())
		require.NoError(t, err)
		if len(cluster) == 2 {
			assert.Equal(t, addr1, cluster[0].Address)
			assert.Equal(t, addr2, cluster[1].Address)
			joined = true
			break
		}
	}

	assert.True(t, joined)
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

func newApp(t *testing.T, options ...app.Option) (*app.App, func()) {
	t.Helper()

	appIndex++

	dir, dirCleanup := newDir(t)

	log := func(l client.LogLevel, format string, a ...interface{}) {
		format = fmt.Sprintf("%d: %s: %s", appIndex, l.String(), format)
		t.Logf(format, a...)
	}

	cert, pool := loadCert(t)
	options = append(options, app.WithLogFunc(log), app.WithTLS(app.SimpleTLSConfig(cert, pool)))

	app, err := app.New(dir, options...)
	require.NoError(t, err)

	cleanup := func() {
		require.NoError(t, app.Close())
		dirCleanup()
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
