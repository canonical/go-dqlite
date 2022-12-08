package dqlite

import (
	"context"
	"time"

	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/go-dqlite/internal/bindings"
	"github.com/pkg/errors"
)

// Node runs a dqlite node.
type Node struct {
	log         client.LogFunc // Logger
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
// DEPRECATED: Use ReconfigureMembership instead, which does not require
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

// ReconfigureMembership can be used to recover a cluster whose majority of
// nodes have died, and therefore has become unavailable.
//
// It forces appending a new configuration to the raft log stored in the given
// directory, effectively replacing the current configuration.
func ReconfigureMembership(dir string, cluster []NodeInfo) error {
	server, err := bindings.NewNode(context.Background(), 1, "1", dir)
	if err != nil {
		return err
	}
	defer server.Close()
	return server.Recover(cluster)
}

// ReconfigureMembershipExt can be used to recover a cluster whose majority of
// nodes have died, and therefore has become unavailable.
//
// It forces appending a new configuration to the raft log stored in the given
// directory, effectively replacing the current configuration.
// In comparision with ReconfigureMembership, this function takes the node role
// into account and makes use of a dqlite API that supports extending the
// NodeInfo struct.
func ReconfigureMembershipExt(dir string, cluster []NodeInfo) error {
	server, err := bindings.NewNode(context.Background(), 1, "1", dir)
	if err != nil {
		return err
	}
	defer server.Close()
	return server.RecoverExt(cluster)
}

// Create a options object with sane defaults.
func defaultOptions() *options {
	return &options{
		DialFunc: client.DefaultDialFunc,
		DiskMode: false, // Be explicit about not enabling disk-mode by default.
	}
}
