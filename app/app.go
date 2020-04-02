package app

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"time"

	"github.com/canonical/go-dqlite"
	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/go-dqlite/driver"
	"github.com/pkg/errors"
)

type App struct {
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

	node, err := dqlite.New(o.ID, o.Address, dir, dqlite.WithBindAddress(o.Address))
	if err != nil {
		return nil, err
	}
	if err := node.Start(); err != nil {
		return nil, err
	}

	store, err := client.DefaultNodeStore(filepath.Join(dir, "servers.sql"))
	if err != nil {
		return nil, err
	}

	store.Set(context.Background(), []client.NodeInfo{{Address: o.Address}})

	driver, err := driver.New(store)
	if err != nil {
		return nil, err
	}
	driverIndex++
	driverName := fmt.Sprintf("dqlite-%d", driverIndex)
	sql.Register(driverName, driver)

	app := &App{
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

var driverIndex = 0
