package app

import "github.com/canonical/go-dqlite"

type App struct {
	node *dqlite.Node
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

	app := &App{
		node: node,
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
