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
	LT
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

// A LeaderTracker stores the address of the last known cluster leader,
// and possibly a reusable connection to it.
type LeaderTracker interface {
	GetLeaderAddr() string
	SetLeaderAddr(string)
	UnsetLeaderAddr()

	// TakeSharedProtocol transfers the reusable connection to the caller,
	// if one is stored.
	TakeSharedProtocol() *Protocol
	// DonateSharedProtocol possibly stores the given connection for reuse.
	//
	// The tracker has only one slot for a reusable connection. If the slot
	// is already occupied, DonateSharedProtocol does nothing and returns
	// false. Otherwise, it adopts the connection and returns true.
	//
	// This is called automatically by Protocol.Close for a connection that
	// was obtained from TakeSharedProtocol. It's okay to call it with a
	// connection that didn't come from TakeSharedProtocol, though.
	DonateSharedProtocol(*Protocol) (accepted bool)
}

// A NodeStoreLeaderTracker is a node store that also tracks the current leader.
type NodeStoreLeaderTracker interface {
	NodeStore
	LeaderTracker
}

// LT implements LeaderTracker and is intended for embedding into a NodeStore.
type LT struct {
	mu                  sync.RWMutex
	lastKnownLeaderAddr string

	proto *Protocol
}

func (lt *LT) GetLeaderAddr() string {
	lt.mu.RLock()
	defer lt.mu.RUnlock()

	return lt.lastKnownLeaderAddr
}

func (lt *LT) SetLeaderAddr(address string) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	lt.lastKnownLeaderAddr = address
}

func (lt *LT) UnsetLeaderAddr() {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	lt.lastKnownLeaderAddr = ""
}

func (lt *LT) TakeSharedProtocol() (proto *Protocol) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	if proto, lt.proto = lt.proto, nil; proto != nil {
		proto.tracker = lt
	}
	return
}

func (lt *LT) DonateSharedProtocol(proto *Protocol) (accepted bool) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	if accepted = lt.proto == nil; accepted {
		lt.proto = proto
	}
	return
}
