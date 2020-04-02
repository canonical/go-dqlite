package app

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/canonical/go-dqlite"
	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/go-dqlite/driver"
	"github.com/pkg/errors"
)

type App struct {
	address    string
	node       *dqlite.Node
	store      client.NodeStore
	driver     *driver.Driver
	driverName string
}

// New creates a new application node.
func New(dir string, options ...Option) (*App, error) {
	o := defaultOptions()
	for _, option := range options {
		option(o)
	}

	// Start the local dqlite engine.
	node, err := dqlite.New(o.ID, o.Address, dir, dqlite.WithBindAddress(o.Address))
	if err != nil {
		return nil, err
	}
	if err := node.Start(); err != nil {
		return nil, err
	}

	// Open the nodes store.
	storePath := filepath.Join(dir, "servers.sql")
	storePathExists := true
	if _, err := os.Stat(storePath); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		storePathExists = false
	}
	store, err := client.DefaultNodeStore(storePath)
	if err != nil {
		return nil, err
	}
	if !storePathExists {
		// If this is a brand new application node, populate the store
		// either with the node's address (for bootstrap nodes) or with
		// the given cluster addresses (for joining nodes).
		nodes := []client.NodeInfo{}
		if o.ID == dqlite.BootstrapID || o.ID == 1 {
			nodes = append(nodes, client.NodeInfo{Address: o.Address})
		} else {
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

	driver, err := driver.New(store)
	if err != nil {
		return nil, err
	}
	driverIndex++
	driverName := fmt.Sprintf("dqlite-%d", driverIndex)
	sql.Register(driverName, driver)

	app := &App{
		address:    o.Address,
		node:       node,
		store:      store,
		driver:     driver,
		driverName: driverName,
	}

	return app, nil
}

// Close the application node, releasing all resources it created.
func (a *App) Close() error {
	if err := a.node.Close(); err != nil {
		return err
	}
	return nil
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
	return client.FindLeader(ctx, a.store)
}

var driverIndex = 0
