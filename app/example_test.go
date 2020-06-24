package app_test

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/canonical/go-dqlite/app"
)

// To start the first node of a dqlite cluster for the first time, its network
// address should be specified using the app.WithAddress() option.
//
// When the node is restarted a second time, the app.WithAddress() option might
// be omitted, since the node address will be persisted in the info.yaml file.
//
// The very first node has always the same ID (dqlite.BootstrapID).
func Example() {
	dir, err := ioutil.TempDir("", "dqlite-app-example-")
	if err != nil {
		return
	}
	defer os.RemoveAll(dir)

	node, err := app.New(dir, app.WithAddress("127.0.0.1:9001"))
	if err != nil {
		return
	}

	fmt.Printf("0x%x %s\n", node.ID(), node.Address())
	// Output: 0x2dc171858c3155be 127.0.0.1:9001

	if err := node.Close(); err != nil {
		return
	}

	node, err = app.New(dir)
	if err != nil {
		return
	}
	defer node.Close()

	fmt.Printf("0x%x %s\n", node.ID(), node.Address())
	// Output: 0x2dc171858c3155be 127.0.0.1:9001
	// 0x2dc171858c3155be 127.0.0.1:9001
}

// After starting the very first node, a second node can be started by passing
// the address of the first node using the app.WithCluster() option.
//
// In general additional nodes can be started by specifying one or more
// addresses of existing nodes using the app.Cluster() option.
//
// When the node is restarted a second time, the app.WithCluster() option might
// be omitted, since the node has already joined the cluster.
//
// Each additional node will be automatically assigned a unique ID.
func ExampleWithCluster() {
	dir1, err := ioutil.TempDir("", "dqlite-app-example-")
	if err != nil {
		return
	}
	defer os.RemoveAll(dir1)

	dir2, err := ioutil.TempDir("", "dqlite-app-example-")
	if err != nil {
		return
	}
	defer os.RemoveAll(dir2)

	dir3, err := ioutil.TempDir("", "dqlite-app-example-")
	if err != nil {
		return
	}
	defer os.RemoveAll(dir3)

	node1, err := app.New(dir1, app.WithAddress("127.0.0.1:9001"))
	if err != nil {
		return
	}
	defer node1.Close()

	node2, err := app.New(dir2, app.WithAddress("127.0.0.1:9002"), app.WithCluster([]string{"127.0.0.1:9001"}))
	if err != nil {
		return
	}
	defer node2.Close()

	node3, err := app.New(dir3, app.WithAddress("127.0.0.1:9003"), app.WithCluster([]string{"127.0.0.1:9001"}))
	if err != nil {
		return
	}

	fmt.Println(node1.ID() != node2.ID(), node1.ID() != node3.ID(), node2.ID() != node3.ID())
	// Output: true true true

	// Restart the third node, the only argument we need to pass to
	// app.New() is its dir.
	id3 := node3.ID()
	if err := node3.Close(); err != nil {
		return
	}

	node3, err = app.New(dir3)
	if err != nil {
		return
	}
	defer node3.Close()

	fmt.Println(node3.ID() == id3, node3.Address())
	// true 127.0.0.1:9003
}
