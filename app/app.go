package app

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ghodss/yaml"

	"github.com/canonical/go-dqlite"
	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/go-dqlite/driver"
	"github.com/pkg/errors"
)

// App is a high-level helper for initializing a typical dqlite-based Go
// application.
//
// It takes care of starting a dqlite node and registering a dqlite Go SQL
// driver.
type App struct {
	id              uint64
	address         string
	node            *dqlite.Node
	nodeBindAddress string
	listener        net.Listener
	tls             *tlsSetup
	store           client.NodeStore
	driver          *driver.Driver
	driverName      string
	log             client.LogFunc
	stop            context.CancelFunc
	serveCh         chan struct{} // Waits for App.serve() to return.
	updateStoreCh   chan struct{} // Waits for App.updateStore() to return.
}

// New creates a new application node.
func New(dir string, options ...Option) (app *App, err error) {
	o := defaultOptions()
	for _, option := range options {
		option(o)
	}

	// List of cleanup functions to run in case of errors.
	cleanups := []func(){}
	defer func() {
		if err == nil {
			return
		}
		for i := range cleanups {
			i = len(cleanups) - 1 - i // Reverse order
			cleanups[i]()
		}
	}()

	// Load our ID, or generate one if we are joining.
	infoPath := filepath.Join(dir, "info.yaml")
	infoPathExists := true
	info := client.NodeInfo{}
	if _, err := os.Stat(infoPath); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("check if info.yaml exists: %w", err)
		}
		infoPathExists = false
		if len(o.Cluster) == 0 {
			info.ID = dqlite.BootstrapID
		} else {
			info.ID = dqlite.GenerateID(o.Address)
		}
		info.Address = o.Address

		data, err := yaml.Marshal(info)
		if err != nil {
			return nil, fmt.Errorf("marshall info.yaml: %w", err)
		}
		if err := ioutil.WriteFile(infoPath, data, 0600); err != nil {
			return nil, fmt.Errorf("write info.yaml: %w", err)
		}

		cleanups = append(cleanups, func() { os.Remove(infoPath) })
	} else {
		data, err := ioutil.ReadFile(infoPath)
		if err != nil {
			return nil, fmt.Errorf("read info.yaml: %w", err)
		}
		if err := yaml.Unmarshal(data, &info); err != nil {
			return nil, fmt.Errorf("unmarshall info.yaml: %w", err)
		}
		if o.Address != "" && o.Address != info.Address {
			return nil, fmt.Errorf("address %q in info.yaml does not match %q", info.Address, o.Address)
		}
	}

	if info.ID == dqlite.BootstrapID && len(o.Cluster) > 0 {
		return nil, fmt.Errorf("bootstrap node can't join a cluster")
	}

	// Start the local dqlite engine.
	var nodeBindAddress string
	var nodeDial client.DialFunc
	if o.TLS != nil {
		nodeBindAddress = fmt.Sprintf("@dqlite-%d", info.ID)

		// Within a snap we need to choose a different name for the abstract unix domain
		// socket to get it past the AppArmor confinement.
		// See https://github.com/snapcore/snapd/blob/master/interfaces/apparmor/template.go#L357
		snapInstanceName := os.Getenv("SNAP_INSTANCE_NAME")
		if len(snapInstanceName) > 0 {
			nodeBindAddress = fmt.Sprintf("@snap.%s.dqlite-%d", snapInstanceName, info.ID)
		}

		nodeDial = makeNodeDialFunc(o.TLS.Dial)
	} else {
		nodeBindAddress = o.Address
		nodeDial = client.DefaultDialFunc
	}
	node, err := dqlite.New(
		info.ID, o.Address, dir,
		dqlite.WithBindAddress(nodeBindAddress),
		dqlite.WithDialFunc(nodeDial),
	)
	if err != nil {
		return nil, fmt.Errorf("create node: %w", err)
	}
	if err := node.Start(); err != nil {
		return nil, fmt.Errorf("start node: %w", err)
	}
	cleanups = append(cleanups, func() { node.Close() })

	// Open the nodes store.
	storePath := filepath.Join(dir, "cluster.yaml")
	storePathExists := true
	if _, err := os.Stat(storePath); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("check if cluster.yaml exists: %w", err)
		}
		storePathExists = false
	}
	store, err := client.NewYamlNodeStore(storePath)
	if err != nil {
		return nil, fmt.Errorf("open cluster.yaml node store: %w", err)
	}

	if !storePathExists {
		// If this is a brand new application node, populate the store
		// either with the node's address (for bootstrap nodes) or with
		// the given cluster addresses (for joining nodes).
		nodes := []client.NodeInfo{{Address: o.Address}}
		if info.ID != dqlite.BootstrapID {
			if len(o.Cluster) == 0 {
				return nil, fmt.Errorf("no cluster addresses provided")
			}
			for _, address := range o.Cluster {
				nodes = append(nodes, client.NodeInfo{Address: address})
			}
		}
		if err := store.Set(context.Background(), nodes); err != nil {
			return nil, fmt.Errorf("initialize node store: %w", err)
		}
		cleanups = append(cleanups, func() { os.Remove(storePath) })
	}

	// Register the local dqlite driver.
	driverDial := client.DefaultDialFunc
	if o.TLS != nil {
		driverDial = client.DialFuncWithTLS(driverDial, o.TLS.Dial)
	}

	driver, err := driver.New(store, driver.WithDialFunc(driverDial), driver.WithLogFunc(o.Log))
	if err != nil {
		return nil, fmt.Errorf("create driver: %w", err)
	}
	driverIndex++
	driverName := fmt.Sprintf("dqlite-%d", driverIndex)
	sql.Register(driverName, driver)

	ctx, stop := context.WithCancel(context.Background())

	app = &App{
		id:              info.ID,
		address:         o.Address,
		node:            node,
		nodeBindAddress: nodeBindAddress,
		store:           store,
		driver:          driver,
		driverName:      driverName,
		log:             o.Log,
		tls:             o.TLS,
		stop:            stop,
		updateStoreCh:   make(chan struct{}, 0),
	}

	if o.TLS != nil {
		listener, err := net.Listen("tcp", o.Address)
		if err != nil {
			return nil, fmt.Errorf("listen to %s: %w", o.Address, err)
		}
		serveCh := make(chan struct{}, 0)

		app.listener = listener
		app.serveCh = serveCh

		go app.serve()

		cleanups = append(cleanups, func() { listener.Close(); <-serveCh })

	}

	// If are starting a brand new non-bootstrap node, let's add it to the
	// cluster.
	if !infoPathExists && info.ID != dqlite.BootstrapID {
		// TODO: add a customizable timeout
		cli, err := app.Leader(context.Background())
		if err != nil {
			return nil, fmt.Errorf("find cluster leader: %w", err)
		}
		defer cli.Close()

		err = cli.Add(
			context.Background(),
			client.NodeInfo{ID: info.ID, Address: o.Address, Role: client.Voter})
		if err != nil {
			return nil, fmt.Errorf("add node to cluster: %w", err)
		}

	}

	go app.updateStore(ctx)

	return app, nil
}

// Close the application node, releasing all resources it created.
func (a *App) Close() error {
	// Stop the store update task.
	a.stop()
	<-a.updateStoreCh

	// Try to transfer leadership if we are the leader.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	cli, err := client.New(ctx, a.nodeBindAddress)
	if err == nil {
		leader, err := cli.Leader(ctx)
		if err == nil && leader != nil && leader.Address == a.address {
			cli.Transfer(ctx, 0)
		}
		cli.Close()
	}

	if a.listener != nil {
		a.listener.Close()
		<-a.serveCh
	}
	if err := a.node.Close(); err != nil {
		return err
	}
	return nil
}

// ID returns the dqlite ID of this application node.
func (a *App) ID() uint64 {
	return a.id
}

// Address returns the dqlite address of this application node.
func (a *App) Address() string {
	return a.address
}

// Driver returns the name used to register the dqlite driver.
func (a *App) Driver() string {
	return a.driverName
}

// Open the dqlite database with the given name
func (a *App) Open(ctx context.Context, database string) (*sql.DB, error) {
	db, err := sql.Open(a.Driver(), database)
	if err != nil {
		return nil, err
	}

	for i := 0; i < 60; i++ {
		err = db.PingContext(ctx)
		if err == nil {
			break
		}
		cause := errors.Cause(err)
		if cause != driver.ErrNoAvailableLeader {
			return nil, err
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		return nil, err
	}

	return db, nil
}

// Leader returns a client connected to the current cluster leader, if any.
func (a *App) Leader(ctx context.Context) (*client.Client, error) {
	dial := client.DefaultDialFunc
	if a.tls != nil {
		dial = client.DialFuncWithTLS(dial, a.tls.Dial)
	}
	return client.FindLeader(ctx, a.store, client.WithDialFunc(dial), client.WithLogFunc(a.log))
}

// Proxy incoming TLS connections.
func (a *App) serve() {
	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	for {
		client, err := a.listener.Accept()
		if err != nil {
			cancel()
			wg.Wait()
			close(a.serveCh)
			return
		}
		address := client.RemoteAddr()
		a.debug("new connection from %s", address)
		server, err := net.Dial("unix", a.nodeBindAddress)
		if err != nil {
			a.error("dial local node: %v", err)
			client.Close()
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := proxy(ctx, client, server, a.tls.Listen); err != nil {
				a.error("proxy: %v", err)
			}
		}()
	}
}

// Periodically update the node store cache.
func (a *App) updateStore(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			close(a.updateStoreCh)
			return
		case <-time.After(30 * time.Second):
			cli, err := a.Leader(ctx)
			if err != nil {
				continue
			}
			servers, err := cli.Cluster(ctx)
			if err != nil {
				continue
			}
			a.store.Set(ctx, servers)
		}
	}
}

func (a *App) debug(format string, args ...interface{}) {
	a.log(client.LogDebug, format, args...)
}

func (a *App) info(format string, args ...interface{}) {
	a.log(client.LogInfo, format, args...)
}

func (a *App) warn(format string, args ...interface{}) {
	a.log(client.LogWarn, format, args...)
}

func (a *App) error(format string, args ...interface{}) {
	a.log(client.LogError, format, args...)
}

var driverIndex = 0
