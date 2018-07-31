package bindings_test

import (
	"testing"

	"github.com/CanonicalLtd/go-dqlite/internal/bindings"
	"github.com/stretchr/testify/require"
)

func TestNewCluster(t *testing.T) {
	defer bindings.AssertNoMemoryLeaks(t)

	methods := &testClusterMethods{}

	cluster, err := bindings.NewCluster(methods)
	require.NoError(t, err)

	cluster.Close()

}

type testClusterMethods struct {
}

func (c *testClusterMethods) Leader() string {
	return "127.0.0.1:666"
}

func (c *testClusterMethods) Servers() ([]bindings.ServerInfo, error) {
	servers := []bindings.ServerInfo{
		{ID: 1, Address: "1.2.3.4:666"},
		{ID: 2, Address: "5.6.7.8:666"},
	}

	return servers, nil
}

func (c *testClusterMethods) Register(*bindings.Conn) {
}

func (c *testClusterMethods) Unregister(*bindings.Conn) {
}

func (c *testClusterMethods) Barrier() error {
	return nil
}

func (c *testClusterMethods) Recover(token uint64) error {
	return nil
}

func (c *testClusterMethods) Checkpoint(*bindings.Conn) error {
	return nil
}
