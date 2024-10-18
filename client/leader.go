package client

import (
	"context"
)

// FindLeader returns a Client connected to the current cluster leader.
//
// The function will iterate through to all nodes in the given store, and for
// each of them check if it's the current leader. If no leader is found, the
// function will keep retrying (with a capped exponential backoff) until the
// given context is canceled.
func FindLeader(ctx context.Context, store NodeStore, options ...Option) (*Client, error) {
	return NewLeaderConnector(store, options...).Connect(ctx)
}
