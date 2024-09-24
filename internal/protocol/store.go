package protocol

import (
	"context"
	"sync"
)

// NodeRole identifies the role of a node.
type NodeRole int

// String implements the Stringer interface.
func (r NodeRole) String() string {
	switch r {
	case Voter:
		return "voter"
	case StandBy:
		return "stand-by"
	case Spare:
		return "spare"
	default:
		return "unknown role"
	}
}

// NodeInfo holds information about a single server.
type NodeInfo struct {
	ID      uint64   `yaml:"ID"`
	Address string   `yaml:"Address"`
	Role    NodeRole `yaml:"Role"`
}

// NodeStore is used by a dqlite client to get an initial list of candidate
// dqlite servers that it can dial in order to find a leader server to connect
// to.
//
// Once connected, the client periodically updates the server addresses in the
// store by querying the leader about changes in the cluster (such as servers
// being added or removed).
type NodeStore interface {
	// Get return the list of known servers.
	Get(context.Context) ([]NodeInfo, error)

	// Set updates the list of known cluster servers.
	Set(context.Context, []NodeInfo) error
}

// InmemNodeStore keeps the list of servers in memory.
type InmemNodeStore struct {
	Compass
	mu      sync.RWMutex
	servers []NodeInfo
}

// NewInmemNodeStore creates NodeStore which stores its data in-memory.
func NewInmemNodeStore() *InmemNodeStore {
	return &InmemNodeStore{
		mu:      sync.RWMutex{},
		servers: make([]NodeInfo, 0),
	}
}

// Get the current servers.
func (i *InmemNodeStore) Get(ctx context.Context) ([]NodeInfo, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	ret := make([]NodeInfo, len(i.servers))
	copy(ret, i.servers)
	return ret, nil
}

// Set the servers.
func (i *InmemNodeStore) Set(ctx context.Context, servers []NodeInfo) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.servers = servers
	return nil
}

// Session is a connection to a dqlite server with some attached metadata.
//
// The additional metadata is used to reuse the connection when possible.
type Session struct {
	Protocol *Protocol
	// The address of the server this session is connected to.
	Address string
	// Tracker points back to the LeaderTracker from which this session was leased,
	// if any.
	Tracker LeaderTracker
}

// Bad marks the session as bad, so that it won't be reused.
func (sess *Session) Bad() {
	sess.Protocol.mu.Lock()
	defer sess.Protocol.mu.Unlock()

	sess.Tracker = nil
}

// Close returns the session to its parent tracker if appropriate,
// or closes the underlying connection otherwise.
func (sess *Session) Close() error {
	if tr := sess.Tracker; tr != nil {
		return tr.Unlease(sess)
	}
	return sess.Protocol.Close()
}

// A LeaderTracker stores the address of the last known cluster leader,
// and possibly a reusable connection to it.
type LeaderTracker interface {
	// Guess returns the address of the last known leader, or nil if none has been recorded.
	Guess() string
	// Point records the address of the current leader.
	Point(string)
	// Shake unsets the recorded leader address.
	Shake()

	// Lease returns an existing session against a node that was once the leader,
	// or nil if no existing session is available.
	//
	// The caller should not assume that the session's connection is still valid,
	// that the remote node is still the leader, or that any particular operations
	// have previously been performed on the session.
	// When closed, the session will be returned to this tracker, unless
	// another session has taken its place in the tracker's session slot
	// or the session was marked as bad.
	Lease() *Session
	// Unlease passes ownership of a session to the tracker.
	//
	// The session need not have been obtained from a call to Lease.
	// It will be made available for reuse by future calls to Lease.
	Unlease(*Session) error
}

// A NodeStoreLeaderTracker is a node store that also tracks the current leader.
type NodeStoreLeaderTracker interface {
	NodeStore
	LeaderTracker
}

// Compass can be used to embed LeaderTracker functionality in another type.
type Compass struct {
	mu                  sync.RWMutex
	lastKnownLeaderAddr string

	session *Session
}

func (co *Compass) Guess() string {
	co.mu.RLock()
	defer co.mu.RUnlock()

	return co.lastKnownLeaderAddr
}

func (co *Compass) Point(address string) {
	co.mu.Lock()
	defer co.mu.Unlock()

	co.lastKnownLeaderAddr = address
}

func (co *Compass) Shake() {
	co.mu.Lock()
	defer co.mu.Unlock()

	co.lastKnownLeaderAddr = ""
}

func (co *Compass) Lease() (sess *Session) {
	co.mu.Lock()
	defer co.mu.Unlock()

	if sess, co.session = co.session, nil; sess != nil {
		sess.Tracker = co
	}
	return
}

func (co *Compass) Unlease(sess *Session) error {
	co.mu.Lock()

	if co.session == nil {
		co.session = sess
		co.mu.Unlock()
		return nil
	} else {
		// Another call to Unlease has already filled the tracker's
		// session slot, so just close this session. (Don't call
		// sess.Close, as that would lead to recursion.) Also, unlock
		// the mutex before closing the session, just so we know
		// that it is never locked for longer than a single assignment.
		co.mu.Unlock()
		return sess.Protocol.Close()
	}
}
