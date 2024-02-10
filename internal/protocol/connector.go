package protocol

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/Rican7/retry"
	"github.com/Rican7/retry/backoff"
	"github.com/Rican7/retry/strategy"
	"github.com/canonical/go-dqlite/logging"
	"github.com/pkg/errors"
)

// DialFunc is a function that can be used to establish a network connection.
type DialFunc func(context.Context, string) (net.Conn, error)

// Connector is in charge of creating a dqlite SQL client connected to the
// current leader of a cluster.
type Connector struct {
	id     uint64       // Conn ID to use when registering against the server.
	store  NodeStore    // Used to get and update current cluster servers.
	config Config       // Connection parameters.
	log    logging.Func // Logging function.
}

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

	connector := &Connector{
		id:     id,
		store:  store,
		config: config,
		log:    log,
	}

	return connector
}

// Connect finds the leader server and returns a connection to it.
//
// If the connector is stopped before a leader is found, nil is returned.
func (c *Connector) Connect(ctx context.Context) (*Protocol, error) {
	var protocol *Protocol

	strategies := makeRetryStrategies(c.config.BackoffFactor, c.config.BackoffCap, c.config.RetryLimit)

	// The retry strategy should be configured to retry indefinitely, until
	// the given context is done.
	err := retry.Retry(func(attempt uint) error {
		log := func(l logging.Level, format string, a ...interface{}) {
			format = fmt.Sprintf("attempt %d: ", attempt) + format
			c.log(l, format, a...)
		}

		select {
		case <-ctx.Done():
			// Stop retrying
			return nil
		default:
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

// Make a single attempt to establish a connection to the leader server trying
// all addresses available in the store.
func (c *Connector) connectAttemptAll(ctx context.Context, log logging.Func) (*Protocol, error) {
	servers, err := c.store.Get(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get servers")
	}

	protocolChan := make(chan *Protocol, len(servers))
	leaderChan := make(chan string, len(servers))

	wg := &sync.WaitGroup{}
	wg.Add(len(servers))

	go func() {
		wg.Wait()
		close(protocolChan)
		close(leaderChan)
	}()

	// Ask each one-hop server who the leader is concurrently
	for _, server := range servers {
		go func(server NodeInfo) {
			defer wg.Done()

			log := func(l logging.Level, format string, a ...interface{}) {
				format = fmt.Sprintf("server %s: ", server.Address) + format
				log(l, format, a...)
			}

			ctx, cancel := context.WithTimeout(ctx, c.config.AttemptTimeout)
			defer cancel()

			protocol, leader, err := c.connectAttemptOne(ctx, server.Address, log)
			if err != nil {
				// This server is unavailable, no result
				log(logging.Warn, err.Error())
				return
			}
			if protocol != nil {
				// We found the leader
				log(logging.Debug, "connected")
				protocolChan <- protocol
				return
			}
			if leader == "" {
				// This server does not know who the current leader is,
				// no result
				log(logging.Warn, "no known leader")
				return
			}

			// This server claims to know the leader
			leaderChan <- leader
		}(server)
	}

	// If one of the connections returned the leader, we're done
	var protocol *Protocol
	for p := range protocolChan {
		if protocol != nil {
			p.Close()
			continue
		}
		protocol = p
	}
	if protocol != nil {
		return protocol, nil
	}

	for leader := range leaderChan {
		// If we get here, it means one-hop servers reported that another
		// server is the leader, let's try connecting to it
		log(logging.Debug, "connect to reported leader %s", leader)

		ctx, cancel := context.WithTimeout(ctx, c.config.AttemptTimeout)
		defer cancel()

		protocol, _, err = c.connectAttemptOne(ctx, leader, log)
		if err != nil {
			// The leader reported by the one-hop server is unavailable
			log(logging.Warn, "reported leader unavailable err=%v", err)
			continue
		}
		if protocol == nil {
			// The leader reported by the one-hop server does not consider itself
			// the leader
			log(logging.Warn, "reported leader server is not the leader")
			continue
		}

		return protocol, nil
	}

	return nil, ErrNoAvailableLeader
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
// Return values:
//
// - Any failure is hit:                     -> nil, "", err
// - Target not leader and no leader known:  -> nil, "", nil
// - Target not leader and leader known:     -> nil, leader, nil
// - Target is the leader:                   -> server, "", nil
func (c *Connector) connectAttemptOne(ctx context.Context, address string, log logging.Func) (*Protocol, string, error) {
	dialCtx, cancel := context.WithTimeout(ctx, c.config.DialTimeout)
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

	// Send the initial Leader request.
	request := Message{}
	request.Init(16)
	response := Message{}
	response.Init(512)

	EncodeLeader(&request)

	if err := protocol.Call(ctx, &request, &response); err != nil {
		protocol.Close()
		cause := errors.Cause(err)
		// Best-effort detection of a pre-1.0 dqlite node: when sent
		// version 1 it should close the connection immediately.
		if err, ok := cause.(*net.OpError); ok && !err.Timeout() || cause == io.EOF {
			return nil, "", errBadProtocol
		}

		return nil, "", err
	}

	_, leader, err := DecodeNodeCompat(protocol, &response)
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
		request.reset()
		response.reset()

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
