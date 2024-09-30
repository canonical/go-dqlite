package protocol

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/Rican7/retry"
	"github.com/canonical/go-dqlite/logging"
	"github.com/pkg/errors"
	"golang.org/x/sync/semaphore"
)

// MaxConcurrentLeaderConns is the default maximum number of concurrent requests to other cluster members to probe for leadership.
const MaxConcurrentLeaderConns int64 = 10

// DialFunc is a function that can be used to establish a network connection.
type DialFunc func(context.Context, string) (net.Conn, error)

// Connector is in charge of creating a dqlite SQL client connected to the
// current leader of a cluster.
type Connector struct {
	id     uint64 // Conn ID to use when registering against the server.
	store  NodeStoreLeaderTracker
	config Config       // Connection parameters.
	log    logging.Func // Logging function.
}

// nonTracking extends any NodeStore with no-op leader tracking.
//
// This is used as a fallback when the NodeStore used by the connector doesn't
// implement NodeStoreLeaderTracker. This can only be the case for a custom NodeStore
// provided by downstream. In this case, the connector will behave as it did before
// the LeaderTracker optimizations were introduced.
type nonTracking struct{ NodeStore }

func (nt *nonTracking) GetLeaderAddr() string               { return "" }
func (nt *nonTracking) SetLeaderAddr(string)                {}
func (nt *nonTracking) UnsetLeaderAddr()                    {}
func (nt *nonTracking) TakeSharedProtocol() *Protocol       { return nil }
func (nt *nonTracking) DonateSharedProtocol(*Protocol) bool { return false }

// NewConnector returns a new connector that can be used by a dqlite driver to
// create new clients connected to a leader dqlite server.
func NewConnector(id uint64, store NodeStore, config Config, log logging.Func) *Connector {
	if config.Dial == nil {
		config.Dial = Dial
	}

	if config.DialTimeout == 0 {
		config.DialTimeout = 5 * time.Second
	}

	if config.AttemptTimeout == 0 {
		config.AttemptTimeout = 15 * time.Second
	}

	if config.BackoffFactor == 0 {
		config.BackoffFactor = 100 * time.Millisecond
	}

	if config.BackoffCap == 0 {
		config.BackoffCap = time.Second
	}

	if config.ConcurrentLeaderConns == 0 {
		config.ConcurrentLeaderConns = MaxConcurrentLeaderConns
	}

	nslt, ok := store.(NodeStoreLeaderTracker)
	if !ok {
		nslt = &nonTracking{store}
	}

	return &Connector{id: id, store: nslt, config: config, log: log}
}

// Connect returns a connection to the cluster leader.
//
// If the connector was configured with PermitShared, and a reusable connection
// is available from the leader tracker, that connection is returned. Otherwise,
// a new connection is opened.
func (c *Connector) Connect(ctx context.Context) (*Protocol, error) {
	if c.config.PermitShared {
		if sharedProto := c.store.TakeSharedProtocol(); sharedProto != nil {
			if leaderAddr, err := askLeader(ctx, sharedProto); err == nil && sharedProto.addr == leaderAddr {
				c.log(logging.Debug, "reusing shared connection to %s", sharedProto.addr)
				c.store.SetLeaderAddr(leaderAddr)
				return sharedProto, nil
			}
			c.log(logging.Debug, "discarding shared connection to %s", sharedProto.addr)
			sharedProto.Bad()
			sharedProto.Close()
		}
	}

	var proto *Protocol
	err := retry.Retry(func(attempt uint) error {
		log := func(l logging.Level, format string, a ...interface{}) {
			format = fmt.Sprintf("attempt %d: ", attempt) + format
			c.log(l, format, a...)
		}

		if attempt > 1 {
			select {
			case <-ctx.Done():
				// Stop retrying
				return nil
			default:
			}
		}

		var err error
		proto, err = c.connectAttemptAll(ctx, log)
		return err
	}, c.config.RetryStrategies()...)

	if err != nil || ctx.Err() != nil {
		return nil, ErrNoAvailableLeader
	}

	// At this point we should have a connected protocol object, since the
	// retry loop didn't hit any error and the given context hasn't
	// expired.
	if proto == nil {
		panic("no protocol object")
	}

	c.store.SetLeaderAddr(proto.addr)
	if c.config.PermitShared {
		proto.tracker = c.store
	}

	return proto, nil
}

// connectAttemptAll tries to establish a new connection to the cluster leader.
//
// First, if the address of the last known leader has been recorded, try
// to connect to that server and confirm its leadership. This is a fast path
// for stable clusters that avoids opening lots of connections. If that fails,
// fall back to probing all servers in parallel, checking whether each
// is the leader itself or knows who the leader is.
func (c *Connector) connectAttemptAll(ctx context.Context, log logging.Func) (*Protocol, error) {
	if addr := c.store.GetLeaderAddr(); addr != "" {
		// TODO In the event of failure, we could still use the second
		// return value to guide the next stage of the search.
		if proto, _, _ := c.connectAttemptOne(ctx, ctx, addr, log); proto != nil {
			log(logging.Debug, "server %s: connected on fast path", addr)
			return proto, nil
		}
		c.store.UnsetLeaderAddr()
	}

	servers, err := c.store.Get(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get servers")
	}
	// Probe voters before standbys before spares. Only voters can potentially
	// be the leader, and standbys are more likely to know who the leader is
	// than spares since they participate more in the cluster.
	sort.Slice(servers, func(i, j int) bool {
		return servers[i].Role < servers[j].Role
	})

	// The new context will be cancelled when we successfully connect
	// to the leader. The original context will be used only for net.Dial.
	// Motivation: threading the cancellation through to net.Dial results
	// in lots of warnings being logged on remote nodes when our probing
	// goroutines disconnect during a TLS handshake.
	origCtx := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	leaderCh := make(chan *Protocol)
	sem := semaphore.NewWeighted(c.config.ConcurrentLeaderConns)
	wg := &sync.WaitGroup{}
	wg.Add(len(servers))
	go func() {
		wg.Wait()
		close(leaderCh)
	}()
	for _, server := range servers {
		go func(server NodeInfo) {
			defer wg.Done()

			if err := sem.Acquire(ctx, 1); err != nil {
				return
			}
			defer sem.Release(1)

			proto, leader, err := c.connectAttemptOne(origCtx, ctx, server.Address, log)
			if err != nil {
				log(logging.Warn, "server %s: %v", server.Address, err)
				return
			} else if proto != nil {
				leaderCh <- proto
				return
			} else if leader == "" {
				log(logging.Warn, "server %s: no known leader", server.Address)
				return
			}

			// Try the server that the original server thinks is the leader.
			log(logging.Debug, "server %s: connect to reported leader %s", server.Address, leader)
			proto, _, err = c.connectAttemptOne(origCtx, ctx, leader, log)
			if err != nil {
				log(logging.Warn, "server %s: %v", leader, err)
				return
			} else if proto == nil {
				log(logging.Warn, "server %s: reported leader server is not the leader", leader)
				return
			}
			leaderCh <- proto
		}(server)
	}

	leader, ok := <-leaderCh
	cancel()
	if !ok {
		return nil, ErrNoAvailableLeader
	}
	log(logging.Debug, "server %s: connected on fallback path", leader.addr)
	for extra := range leaderCh {
		extra.Close()
	}
	return leader, nil
}

// Perform the initial handshake using the given protocol version.
func Handshake(ctx context.Context, conn net.Conn, version uint64, addr string) (*Protocol, error) {
	// Latest protocol version.
	protocol := make([]byte, 8)
	binary.LittleEndian.PutUint64(protocol, version)

	// Honor the ctx deadline, if present.
	if deadline, ok := ctx.Deadline(); ok {
		conn.SetDeadline(deadline)
		defer conn.SetDeadline(time.Time{})
	}

	// Perform the protocol handshake.
	n, err := conn.Write(protocol)
	if err != nil {
		return nil, errors.Wrap(err, "write handshake")
	}
	if n != 8 {
		return nil, errors.Wrap(io.ErrShortWrite, "short handshake write")
	}

	return &Protocol{conn: conn, version: version, addr: addr}, nil
}

// Connect to the given dqlite server and check if it's the leader.
//
// dialCtx is used for net.Dial; ctx is used for all other requests.
//
// Return values:
//
// - Any failure is hit:                     -> nil, "", err
// - Target not leader and no leader known:  -> nil, "", nil
// - Target not leader and leader known:     -> nil, leader, nil
// - Target is the leader:                   -> server, "", nil
func (c *Connector) connectAttemptOne(
	dialCtx context.Context,
	ctx context.Context,
	address string,
	log logging.Func,
) (*Protocol, string, error) {
	log = func(l logging.Level, format string, a ...interface{}) {
		format = fmt.Sprintf("server %s: ", address) + format
		log(l, format, a...)
	}

	if ctx.Err() != nil {
		return nil, "", ctx.Err()
	}

	ctx, cancel := context.WithTimeout(ctx, c.config.AttemptTimeout)
	defer cancel()

	dialCtx, cancel = context.WithTimeout(dialCtx, c.config.DialTimeout)
	defer cancel()

	// Establish the connection.
	conn, err := c.config.Dial(dialCtx, address)
	if err != nil {
		return nil, "", errors.Wrap(err, "dial")
	}

	version := VersionOne
	protocol, err := Handshake(ctx, conn, version, address)
	if err == errBadProtocol {
		log(logging.Warn, "unsupported protocol %d, attempt with legacy", version)
		version = VersionLegacy
		protocol, err = Handshake(ctx, conn, version, address)
	}
	if err != nil {
		conn.Close()
		return nil, "", err
	}

	leader, err := askLeader(ctx, protocol)
	if err != nil {
		protocol.Close()
		return nil, "", err
	}
	switch leader {
	case "":
		// Currently this server does not know about any leader.
		protocol.Close()
		return nil, "", nil
	case address:
		// This server is the leader, register ourselves and return.
		request := Message{}
		request.Init(16)
		response := Message{}
		response.Init(512)

		EncodeClient(&request, c.id)

		if err := protocol.Call(ctx, &request, &response); err != nil {
			protocol.Close()
			return nil, "", err
		}

		_, err := DecodeWelcome(&response)
		if err != nil {
			protocol.Close()
			return nil, "", err
		}

		// TODO: enable heartbeat
		// protocol.heartbeatTimeout = time.Duration(heartbeatTimeout) * time.Millisecond
		// go protocol.heartbeat()

		return protocol, "", nil
	default:
		// This server claims to know who the current leader is.
		protocol.Close()
		return nil, leader, nil
	}
}

// TODO move client logic including Leader method to Protocol,
// and get rid of this.
func askLeader(ctx context.Context, protocol *Protocol) (string, error) {
	request := Message{}
	request.Init(16)
	response := Message{}
	response.Init(512)

	EncodeLeader(&request)

	if err := protocol.Call(ctx, &request, &response); err != nil {
		cause := errors.Cause(err)
		// Best-effort detection of a pre-1.0 dqlite node: when sent
		// version 1 it should close the connection immediately.
		if err, ok := cause.(*net.OpError); ok && !err.Timeout() || cause == io.EOF {
			return "", errBadProtocol
		}

		return "", err
	}

	_, leader, err := DecodeNodeCompat(protocol, &response)
	if err != nil {
		return "", err
	}
	return leader, nil
}

var errBadProtocol = fmt.Errorf("bad protocol")
