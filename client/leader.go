package client

import (
	"context"

	"github.com/canonical/go-dqlite/internal/protocol"
)

// FindLeader returns a Client connected to the current cluster leader.
//
// The function will iterate through to all nodes in the given store, and for
// each of them check if it's the current leader. If no leader is found, the
// function will keep retrying (with a capped exponential backoff) until the
// given context is canceled.
func FindLeader(ctx context.Context, store NodeStore, options ...Option) (*Client, error) {
	o := defaultOptions()

	for _, option := range options {
		option(o)
	}

	config := protocol.Config{
		Dial: o.DialFunc,
	}
	connector := protocol.NewConnector(0, store, config, o.LogFunc)
	protocol, err := connector.Connect(ctx)
	if err != nil {
		return nil, err
	}

	client := &Client{protocol: protocol}

	return client, nil
}
