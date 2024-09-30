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
	"github.com/Rican7/retry/backoff"
	"github.com/Rican7/retry/strategy"
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

type nonTracking struct{ NodeStore }

func (nt *nonTracking) Guess() string          { return "" }
func (nt *nonTracking) Point(string)           {}
func (nt *nonTracking) Shake()                 {}
func (nt *nonTracking) Lease() *Session        { return nil }
func (nt *nonTracking) Unlease(*Session) error { return nil }

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

	connector := &Connector{
		id:     id,
		store:  nslt,
		config: config,
		log:    log,
	}

	return connector
}

// Connect finds the leader server and returns a connection to it.
//
// If the connector is stopped before a leader is found, nil is returned.
func (c *Connector) Connect(ctx context.Context) (*Session, error) {
	var protocol *Session

	if c.config.PermitShared {
		sess := c.store.Lease()
		if sess != nil {
			leader, err := askLeader(ctx, sess.Protocol)
			if err == nil && sess.Address == leader {
				c.log(logging.Debug, "reusing shared connection to %s", sess.Address)
				return sess, nil
			}
			c.log(logging.Debug, "discarding shared connection to %s", sess.Address)
			sess.Bad()
			sess.Close()
		}
	}

	strategies := makeRetryStrategies(c.config.BackoffFactor, c.config.BackoffCap, c.config.RetryLimit)

	// The retry strategy should be configured to retry indefinitely, until
	// the given context is done.
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
		protocol, err = c.connectAttemptAll(ctx, log)
		if err != nil {
			return err
		}

		return nil
	}, strategies...)

	if err != nil {
		// We exhausted the number of retries allowed by the configured
		// strategy.
		return nil, ErrNoAvailableLeader
	}

	if ctx.Err() != nil {
		return nil, ErrNoAvailableLeader
	}

	// At this point we should have a connected protocol object, since the
	// retry loop didn't hit any error and the given context hasn't
	// expired.
	if protocol == nil {
		panic("no protocol object")
	}

	return protocol, nil
}

func (c *Connector) connectAttemptAll(ctx context.Context, log logging.Func) (*Session, error) {
	if addr := c.store.Guess(); addr != "" {
		// TODO In the event of failure, we could still use the second
		// return value to guide the next stage of the search.
		if p, _, _ := c.connectAttemptOne(ctx, ctx, addr, log); p != nil {
			log(logging.Debug, "server %s: connected on fast path", addr)
			return &Session{Protocol: p, Address: addr}, nil
		}
		c.store.Shake()
	}

	servers, err := c.store.Get(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get servers")
	}

	// Sort servers by Role, from low to high.
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

	sem := semaphore.NewWeighted(c.config.ConcurrentLeaderConns)

	leaderCh := make(chan *Session)

	wg := &sync.WaitGroup{}
	wg.Add(len(servers))

	go func() {
		wg.Wait()
		close(leaderCh)
	}()

	// Make an attempt for each address until we find the leader.
	for _, server := range servers {
		go func(server NodeInfo, pc chan<- *Session) {
			defer wg.Done()

			if err := sem.Acquire(ctx, 1); err != nil {
				return
			}
			defer sem.Release(1)

			if ctx.Err() != nil {
				return
			}

			protocol, leader, err := c.connectAttemptOne(origCtx, ctx, server.Address, log)
			if err != nil {
				// This server is unavailable, try with the next target.
				log(logging.Warn, "server %s: %s", server.Address, err.Error())
				return
			}
			if protocol != nil {
				// We found the leader
				pc <- &Session{Protocol: protocol, Address: server.Address}
				return
			}
			if leader == "" {
				// This server does not know who the current leader is,
				// try with the next target.
				log(logging.Warn, "server %s: no known leader", server.Address)
				return
			}

			// If we get here, it means this server reported that another
			// server is the leader, let's close the connection to this
			// server and try with the suggested one.
			log(logging.Debug, "server %s: connect to reported leader %s", server.Address, leader)

			protocol, _, err = c.connectAttemptOne(origCtx, ctx, leader, log)
			if err != nil {
				// The leader reported by the previous server is
				// unavailable, try with the next target.
				log(logging.Warn, "server %s: reported leader unavailable err=%v", leader, err)
				return
			}
			if protocol == nil {
				// The leader reported by the target server does not consider itself
				// the leader, try with the next target.
				log(logging.Warn, "server %s: reported leader server is not the leader", leader)
				return
			}
			pc <- &Session{Protocol: protocol, Address: leader}
		}(server, leaderCh)
	}

	// Read from protocol chan, cancel context
	leader, ok := <-leaderCh
	if !ok {
		return nil, ErrNoAvailableLeader
	}
	log(logging.Debug, "server %s: connected on fallback path", leader.Address)
	c.store.Point(leader.Address)
	leader.Tracker = c.store

	cancel()

	for extra := range leaderCh {
		extra.Close()
	}

	return leader, nil
}

// Perform the initial handshake using the given protocol version.
func Handshake(ctx context.Context, conn net.Conn, version uint64) (*Protocol, error) {
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

	return newProtocol(version, conn), nil
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
	protocol, err := Handshake(ctx, conn, version)
	if err == errBadProtocol {
		log(logging.Warn, "unsupported protocol %d, attempt with legacy", version)
		version = VersionLegacy
		protocol, err = Handshake(ctx, conn, version)
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

// Return a retry strategy with exponential backoff, capped at the given amount
// of time and possibly with a maximum number of retries.
func makeRetryStrategies(factor, cap time.Duration, limit uint) []strategy.Strategy {
	limit += 1 // Fix for change in behavior: https://github.com/Rican7/retry/pull/12
	backoff := backoff.BinaryExponential(factor)

	strategies := []strategy.Strategy{}

	if limit > 1 {
		strategies = append(strategies, strategy.Limit(limit))
	}

	strategies = append(strategies,
		func(attempt uint) bool {
			if attempt > 0 {
				duration := backoff(attempt)
				// Duration might be negative in case of integer overflow.
				if duration > cap || duration <= 0 {
					duration = cap
				}
				time.Sleep(duration)
			}

			return true
		},
	)

	return strategies
}

var errBadProtocol = fmt.Errorf("bad protocol")
