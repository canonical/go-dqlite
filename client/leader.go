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
	return NewLeaderConnector(store, options...).Find(ctx)
}

type LeaderConnector protocol.Connector

func NewLeaderConnector(store NodeStore, options ...Option) *LeaderConnector {
	o := defaultOptions()

	for _, option := range options {
		option(o)
	}

	config := protocol.Config{
		Dial:                  o.DialFunc,
		ConcurrentLeaderConns: o.ConcurrentLeaderConns,
	}
	pc := protocol.NewConnector(0, store, config, o.LogFunc)
	return (*LeaderConnector)(pc)
}

func (lc *LeaderConnector) Find(ctx context.Context) (*Client, error) {
	proto, err := (*protocol.Connector)(lc).ConnectPermitShared(ctx)
	if err != nil {
		return nil, err
	}
	return &Client{proto}, nil
}
