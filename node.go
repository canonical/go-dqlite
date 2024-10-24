package dqlite

import (
	"context"
	"time"

	"github.com/canonical/go-dqlite/v3/client"
	"github.com/canonical/go-dqlite/v3/internal/bindings"
	"github.com/pkg/errors"
)

// Node runs a dqlite node.
type Node struct {
	server      *bindings.Node // Low-level C implementation
	acceptCh    chan error     // Receives connection handling errors
	id          uint64
	address     string
	bindAddress string
	cancel      context.CancelFunc
}

// NodeInfo is a convenience alias for client.NodeInfo.
type NodeInfo = client.NodeInfo

// SnapshotParams exposes bindings.SnapshotParams. Used for setting dqlite's
// snapshot parameters.
// SnapshotParams.Threshold controls after how many raft log entries a snapshot is
// taken. The higher this number, the lower the frequency of the snapshots.
// SnapshotParams.Trailing controls how many raft log entries are retained after
// taking a snapshot.
type SnapshotParams = bindings.SnapshotParams

// Option can be used to tweak node parameters.
type Option func(*options)

// WithDialFunc sets a custom dial function for the server.
func WithDialFunc(dial client.DialFunc) Option {
	return func(options *options) {
		options.DialFunc = dial
	}
}

// WithBindAddress sets a custom bind address for the server.
func WithBindAddress(address string) Option {
	return func(options *options) {
		options.BindAddress = address
	}
}

// WithNetworkLatency sets the average one-way network latency.
func WithNetworkLatency(latency time.Duration) Option {
	return func(options *options) {
		options.NetworkLatency = uint64(latency.Nanoseconds())
	}
}

// WithFailureDomain sets the code of the failure domain the node belongs to.
func WithFailureDomain(code uint64) Option {
	return func(options *options) {
		options.FailureDomain = code
	}
}

// WithSnapshotParams sets the snapshot parameters of the node.
func WithSnapshotParams(params SnapshotParams) Option {
	return func(options *options) {
		options.SnapshotParams = params
	}
}

// WithDiskMode enables dqlite disk-mode on the node.
// WARNING: This is experimental API, use with caution
// and prepare for data loss.
// UNSTABLE: Behavior can change in future.
// NOT RECOMMENDED for production use-cases, use at own risk.
func WithDiskMode(disk bool) Option {
	return func(options *options) {
		options.DiskMode = disk
	}
}

// WithAutoRecovery enables or disables auto-recovery of persisted data
// at startup for this node.
//
// When auto-recovery is enabled, raft snapshots and segment files may be
// deleted at startup if they are determined to be corrupt. This helps
// the startup process to succeed in more cases, but can lead to data loss.
//
// Auto-recovery is enabled by default.
func WithAutoRecovery(recovery bool) Option {
	return func(options *options) {
		options.AutoRecovery = recovery
	}
}

// New creates a new Node instance.
func New(id uint64, address string, dir string, options ...Option) (*Node, error) {
	o := defaultOptions()

	for _, option := range options {
		option(o)
	}

	ctx, cancel := context.WithCancel(context.Background())
	server, err := bindings.NewNode(ctx, id, address, dir)
	if err != nil {
		cancel()
		return nil, err
	}

	if o.DialFunc != nil {
		if err := server.SetDialFunc(o.DialFunc); err != nil {
			cancel()
			return nil, err
		}
	}
	if o.BindAddress != "" {
		if err := server.SetBindAddress(o.BindAddress); err != nil {
			cancel()
			return nil, err
		}
	}
	if o.NetworkLatency != 0 {
		if err := server.SetNetworkLatency(o.NetworkLatency); err != nil {
			cancel()
			return nil, err
		}
	}
	if o.FailureDomain != 0 {
		if err := server.SetFailureDomain(o.FailureDomain); err != nil {
			cancel()
			return nil, err
		}
	}
	if o.SnapshotParams.Threshold != 0 || o.SnapshotParams.Trailing != 0 {
		if err := server.SetSnapshotParams(o.SnapshotParams); err != nil {
			cancel()
			return nil, err
		}
	}
	if o.DiskMode {
		if err := server.EnableDiskMode(); err != nil {
			cancel()
			return nil, err
		}
	}
	if !o.AutoRecovery {
		if err := server.SetAutoRecovery(false); err != nil {
			cancel()
			return nil, err
		}
	}

	s := &Node{
		server:      server,
		acceptCh:    make(chan error, 1),
		id:          id,
		address:     address,
		bindAddress: o.BindAddress,
		cancel:      cancel,
	}

	return s, nil
}

// BindAddress returns the network address the node is listening to.
func (s *Node) BindAddress() string {
	return s.server.GetBindAddress()
}

// Start serving requests.
func (s *Node) Start() error {
	return s.server.Start()
}

// Recover a node by forcing a new cluster configuration.
//
// Deprecated: use ReconfigureMembershipExt instead, which does not require
// instantiating a new Node object.
func (s *Node) Recover(cluster []NodeInfo) error {
	return s.server.Recover(cluster)
}

// Hold configuration options for a dqlite server.
type options struct {
	Log            client.LogFunc
	DialFunc       client.DialFunc
	BindAddress    string
	NetworkLatency uint64
	FailureDomain  uint64
	SnapshotParams bindings.SnapshotParams
	DiskMode       bool
	AutoRecovery   bool
}

// Close the server, releasing all resources it created.
func (s *Node) Close() error {
	s.cancel()
	// Send a stop signal to the dqlite event loop.
	if err := s.server.Stop(); err != nil {
		return errors.Wrap(err, "server failed to stop")
	}

	s.server.Close()

	return nil
}

// BootstrapID is a magic ID that should be used for the fist node in a
// cluster. Alternatively ID 1 can be used as well.
const BootstrapID = 0x2dc171858c3155be

// GenerateID generates a unique ID for a new node, based on a hash of its
// address and the current time.
func GenerateID(address string) uint64 {
	return bindings.GenerateID(address)
}

// ReconfigureMembership forces a new cluster configuration.
//
// Deprecated: this function ignores the provided node roles and makes every
// node in the new configuration a voter. Use ReconfigureMembershipExt, which
// respects the provided roles.
func ReconfigureMembership(dir string, cluster []NodeInfo) error {
	server, err := bindings.NewNode(context.Background(), 1, "1", dir)
	if err != nil {
		return err
	}
	defer server.Close()
	return server.Recover(cluster)
}

// ReconfigureMembershipExt forces a new cluster configuration.
//
// This function is useful to revive a cluster that can't achieve quorum in its
// old configuration because some nodes can't be brought online. Forcing a new
// configuration is unsafe, and you should follow these steps to avoid data
// loss and inconsistency:
//
// 1. Make sure no dqlite node in the cluster is running.
// 2. Identify all dqlite nodes that have survived and that you want to be part
//    of the recovered cluster. Call this the "new member list".
// 3. Call ReadLastEntryInfo on each node in the member list, and find which
//    node has the most recent entry according to LastEntryInfo.Before. Call this
//    the "template node".
// 4. Invoke ReconfigureMembershipExt exactly one time, on the template node.
//    The arguments are the data directory of the template node and the new
//    member list.
// 5. Copy the data directory of the template node to all other nodes in the
//    new member list, replacing their previous data directories.
// 6. Restart all nodes in the new member list.
func ReconfigureMembershipExt(dir string, cluster []NodeInfo) error {
	server, err := bindings.NewNode(context.Background(), 1, "1", dir)
	if err != nil {
		return err
	}
	defer server.Close()
	return server.RecoverExt(cluster)
}

// LastEntryInfo holds information about the last entry in the persistent raft
// log of a node.
//
// The zero value is not a valid entry description, and can be used as a
// sentinel.
type LastEntryInfo struct {
	Term, Index uint64
}

// Before tells whether the entry described by the receiver is strictly less
// recent than another entry.
//
// Entry A is less recent than entry B when A has a lower term number, or
// when A and B have the same term number and A has a lower index.
func (lhs LastEntryInfo) Before(rhs LastEntryInfo) bool {
	return lhs.Term < rhs.Term || (lhs.Term == rhs.Term && lhs.Index < rhs.Index)
}

// ReadLastEntryInfo reads information about the last entry in the raft
// persistent log from a node's data directory.
//
// This is intended to be used during the cluster recovery process, see
// ReconfigureMembershipExt. The node must not be running.
//
// This is a non-destructive operation, but is not read-only, since it has the
// side effect of renaming raft open segment files to closed segment files.
func ReadLastEntryInfo(dir string) (LastEntryInfo, error) {
	node, err := bindings.NewNode(context.Background(), 1, "1", dir)
	if err != nil {
		return LastEntryInfo{}, err
	}
	defer node.Close()
	if err = node.SetAutoRecovery(false); err != nil {
		return LastEntryInfo{}, err
	}
	index, term, err := node.DescribeLastEntry()
	if err != nil {
		return LastEntryInfo{}, err
	}
	return LastEntryInfo{term, index}, nil
}

// Create a options object with sane defaults.
func defaultOptions() *options {
	return &options{
		DialFunc: client.DefaultDialFunc,
		DiskMode: false, // Be explicit about not enabling disk-mode by default.
		AutoRecovery: true,
	}
}
