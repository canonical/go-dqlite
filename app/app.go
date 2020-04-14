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
	serveCh         chan struct{} // Waits for App.serve() to return.
}

// New creates a new application node.
func New(dir string, options ...Option) (*App, error) {
	o := defaultOptions()
	for _, option := range options {
		option(o)
	}

	// Load our ID, or generate one if we are joining.
	infoPath := filepath.Join(dir, "info.yaml")
	infoPathExists := true
	info := client.NodeInfo{}
	if _, err := os.Stat(infoPath); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
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
			return nil, err
		}
		if err := ioutil.WriteFile(infoPath, data, 0600); err != nil {
			return nil, err
		}
	} else {
		data, err := ioutil.ReadFile(infoPath)
		if err != nil {
			return nil, err
		}
		if err := yaml.Unmarshal(data, &info); err != nil {
			return nil, err
		}
		if o.Address != "" && o.Address != info.Address {
			return nil, fmt.Errorf("address in info.yaml does not match the given one")
		}
	}

	// Start the local dqlite engine.
	var nodeBindAddress string
	var nodeDial client.DialFunc
	if o.TLS != nil {
		nodeBindAddress = fmt.Sprintf("@dqlite-%d", info.ID)
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
		return nil, err
	}
	if err := node.Start(); err != nil {
		return nil, err
	}

	// Open the nodes store.
	storePath := filepath.Join(dir, "cluster.yaml")
	storePathExists := true
	if _, err := os.Stat(storePath); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		storePathExists = false
	}
	store, err := client.NewYamlNodeStore(storePath)
	if err != nil {
		return nil, err
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
			return nil, err
		}
	}

	// Register the local dqlite driver.
	driverDial := client.DefaultDialFunc
	if o.TLS != nil {
		driverDial = client.DialFuncWithTLS(driverDial, o.TLS.Dial)
	}

	driver, err := driver.New(store, driver.WithDialFunc(driverDial), driver.WithLogFunc(o.Log))
	if err != nil {
		return nil, err
	}
	driverIndex++
	driverName := fmt.Sprintf("dqlite-%d", driverIndex)
	sql.Register(driverName, driver)

	app := &App{
		id:              info.ID,
		address:         o.Address,
		node:            node,
		nodeBindAddress: nodeBindAddress,
		store:           store,
		driver:          driver,
		driverName:      driverName,
		log:             o.Log,
		tls:             o.TLS,
	}

	if o.TLS != nil {
		listener, err := net.Listen("tcp", o.Address)
		if err != nil {
			return nil, err
		}
		app.listener = listener
		app.serveCh = make(chan struct{}, 0)

		go app.serve()
	}

	// If are starting a brand new non-bootstrap node, let's add it to the
	// cluter.
	if !infoPathExists && info.ID != dqlite.BootstrapID {
		// TODO: add a customizable timeout
		cli, err := app.Leader(context.Background())
		if err != nil {
			return nil, err
		}
		err = cli.Add(
			context.Background(),
			client.NodeInfo{ID: info.ID, Address: o.Address, Role: client.Voter})
		if err != nil {
			return nil, err
		}

	}

	return app, nil
}

// Close the application node, releasing all resources it created.
func (a *App) Close() error {
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

// Open the dqlite database with the given name
func (a *App) Open(ctx context.Context, database string) (*sql.DB, error) {
	db, err := sql.Open(a.driverName, database)
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
