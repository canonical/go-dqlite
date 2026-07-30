// Copyright 2017 Canonical Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"math"
	"net"
	"reflect"
	"syscall"
	"time"

	"github.com/pkg/errors"

	"github.com/canonical/go-dqlite/v3/client"
	"github.com/canonical/go-dqlite/v3/internal/protocol"
	"github.com/canonical/go-dqlite/v3/metrics"
	"github.com/canonical/go-dqlite/v3/tracing"
)

// Driver perform queries against a dqlite server.
type Driver struct {
	log                    client.LogFunc   // Log function to use
	store                  client.NodeStore // Holds addresses of dqlite servers
	context                context.Context  // Global cancellation context
	connectionTimeout      time.Duration    // Max time to wait for a new connection
	contextTimeout         time.Duration    // Default client context timeout.
	clientConfig           protocol.Config  // Configuration for dqlite client instances
	concurrentLeaderConns  *int64           // Maximum number of concurrent connections to other cluster members while probing for leadership.
	statementCacheCapacity int              // Maximum cached prepared statements per connection.
	metrics                metrics.Recorder // Metrics recorder shared by all connections.
}

// Error is returned in case of database errors.
type Error = protocol.Error

// Error codes. Values here mostly overlap with native SQLite codes.
const (
	ErrBusy                = 5
	ErrBusyRecovery        = 5 | (1 << 8)
	ErrBusySnapshot        = 5 | (2 << 8)
	errIoErr               = 10
	ErrIoErrNotLeader      = errIoErr | (40 << 8)
	ErrIoErrLeadershipLost = errIoErr | (41 << 8)
	errNotFound            = 12

	// Legacy error codes before version-3.32.1+replication4. Kept here
	// for backward compatibility, but should eventually be dropped.
	errIoErrNotLeaderLegacy      = errIoErr | 32<<8
	errIoErrLeadershipLostLegacy = errIoErr | (33 << 8)
)

// Option can be used to tweak driver parameters.
type Option func(*options)

// NodeStore is a convenience alias of client.NodeStore.
type NodeStore = client.NodeStore

// NodeInfo is a convenience alias of client.NodeInfo.
type NodeInfo = client.NodeInfo

// DefaultNodeStore is a convenience alias of client.DefaultNodeStore.
var DefaultNodeStore = client.DefaultNodeStore

// WithLogFunc sets a custom logging function.
func WithLogFunc(log client.LogFunc) Option {
	return func(options *options) {
		options.Log = log
	}
}

// DialFunc is a function that can be used to establish a network connection
// with a dqlite node.
type DialFunc = protocol.DialFunc

// WithDialFunc sets a custom dial function.
func WithDialFunc(dial DialFunc) Option {
	return func(options *options) {
		options.Dial = protocol.DialFunc(dial)
	}
}

// WithConnectionTimeout sets the connection timeout.
//
// If not used, the default is 5 seconds.
//
// DEPRECATED: Connection cancellation is supported via the driver.Connector
// interface, which is used internally by the stdlib sql package.
func WithConnectionTimeout(timeout time.Duration) Option {
	return func(options *options) {
		options.ConnectionTimeout = timeout
	}
}

// WithConnectionBackoffFactor sets the exponential backoff factor for retrying
// failed connection attempts.
//
// If not used, the default is 100 milliseconds.
func WithConnectionBackoffFactor(factor time.Duration) Option {
	return func(options *options) {
		options.ConnectionBackoffFactor = factor
	}
}

// WithConnectionBackoffCap sets the maximum connection retry backoff value,
// (regardless of the backoff factor) for retrying failed connection attempts.
//
// If not used, the default is 1 second.
func WithConnectionBackoffCap(cap time.Duration) Option {
	return func(options *options) {
		options.ConnectionBackoffCap = cap
	}
}

// WithConcurrentLeaderConns is the maximum number of concurrent connections
// to other cluster members that will be attempted while searching for the dqlite leader.
// It takes a pointer to an integer so that the value can be dynamically modified based on cluster health.
//
// The default is 10 connections to other cluster members.
func WithConcurrentLeaderConns(maxConns *int64) Option {
	return func(o *options) {
		o.ConcurrentLeaderConns = maxConns
	}
}

// WithAttemptTimeout sets the timeout for each individual connection attempt.
//
// The Connector.Connect() and Driver.Open() methods try to find the current
// leader among the servers in the store that was passed to New(). Each time
// they attempt to probe an individual server for leadership this timeout will
// apply, so a server which accepts the connection but it's then unresponsive
// won't block the line.
//
// If not used, the default is 15 seconds.
func WithAttemptTimeout(timeout time.Duration) Option {
	return func(options *options) {
		options.AttemptTimeout = timeout
	}
}

// WithRetryLimit sets the maximum number of connection retries.
//
// If not used, the default is 0 (unlimited retries)
func WithRetryLimit(limit uint) Option {
	return func(options *options) {
		options.RetryLimit = limit
	}
}

// WithContext sets a global cancellation context.
//
// DEPRECATED: This API is no a no-op. Users should explicitly pass a context
// if they wish to cancel their requests.
func WithContext(context context.Context) Option {
	return func(options *options) {
		options.Context = context
	}
}

// WithContextTimeout sets the default client context timeout for DB.Begin()
// when no context deadline is provided.
//
// DEPRECATED: Users should use db APIs that support contexts if they wish to
// cancel their requests.
func WithContextTimeout(timeout time.Duration) Option {
	return func(options *options) {
		options.ContextTimeout = timeout
	}
}

// WithTracing emits a log message at the given level for each statement
// command. Logging is implemented as a metrics recorder and can be enabled
// alongside WithMetrics.
func WithTracing(level client.LogLevel) Option {
	return func(options *options) {
		options.Tracing = level
	}
}

// WithStatementCacheCapacity sets the maximum number of prepared statements
// retained by each connection. The default is 50. A capacity of zero disables
// caching; negative capacities are rejected by New.
func WithStatementCacheCapacity(capacity int) Option {
	return func(options *options) {
		options.StatementCacheCapacity = capacity
	}
}

// WithMetrics sets the recorder used for statement cache and command metrics.
// The recorder must be safe for concurrent use by multiple connections.
func WithMetrics(recorder metrics.Recorder) Option {
	return func(options *options) {
		options.Metrics = recorder
	}
}

// New creates a new dqlite driver, which also implements the
// driver.Driver interface.
func New(store client.NodeStore, options ...Option) (*Driver, error) {
	o := defaultOptions()

	for _, option := range options {
		option(o)
	}
	if o.StatementCacheCapacity < 0 {
		return nil, fmt.Errorf("statement cache capacity must not be negative")
	}
	recorder := o.Metrics
	if o.Tracing != client.LogNone {
		recorder = metrics.Combine(recorder, metrics.NewLogRecorder(o.Log, o.Tracing))
	}

	driver := &Driver{
		log:                    o.Log,
		store:                  store,
		context:                o.Context,
		connectionTimeout:      o.ConnectionTimeout,
		contextTimeout:         o.ContextTimeout,
		concurrentLeaderConns:  o.ConcurrentLeaderConns,
		statementCacheCapacity: o.StatementCacheCapacity,
		metrics:                recorder,
		clientConfig: protocol.Config{
			Dial:           o.Dial,
			AttemptTimeout: o.AttemptTimeout,
			BackoffFactor:  o.ConnectionBackoffFactor,
			BackoffCap:     o.ConnectionBackoffCap,
			RetryLimit:     o.RetryLimit,
		},
	}

	return driver, nil
}

// Hold configuration options for a dqlite driver.
type options struct {
	Log                     client.LogFunc
	Dial                    protocol.DialFunc
	AttemptTimeout          time.Duration
	ConnectionTimeout       time.Duration
	ContextTimeout          time.Duration
	ConnectionBackoffFactor time.Duration
	ConnectionBackoffCap    time.Duration
	ConcurrentLeaderConns   *int64
	RetryLimit              uint
	Context                 context.Context
	Tracing                 client.LogLevel
	StatementCacheCapacity  int
	Metrics                 metrics.Recorder
}

// Create a options object with sane defaults.
func defaultOptions() *options {
	maxConns := protocol.MaxConcurrentLeaderConns
	return &options{
		Log:                    client.DefaultLogFunc,
		Dial:                   client.DefaultDialFunc,
		Tracing:                client.LogNone,
		ConcurrentLeaderConns:  &maxConns,
		StatementCacheCapacity: 100,
	}
}

// A Connector represents a driver in a fixed configuration and can create any
// number of equivalent Conns for use by multiple goroutines.
type Connector struct {
	uri      string
	driver   *Driver
	protocol *protocol.Connector
}

// Connect returns a connection to the database.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	if c.driver.context != nil {
		ctx = c.driver.context
	}

	if c.driver.connectionTimeout != 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, c.driver.connectionTimeout)
		defer cancel()
	}

	conn := &Conn{
		log:            c.driver.log,
		contextTimeout: c.driver.contextTimeout,
		stmtCache:      newStmtCache(c.driver.statementCacheCapacity),
		metrics:        c.driver.metrics,
	}

	proto, err := c.protocol.Connect(ctx)
	if err != nil {
		return nil, driverError(conn.log, errors.Wrap(err, "failed to create dqlite connection"))
	}
	conn.protocol = proto

	conn.request.Init(4096)
	conn.response.Init(4096)

	protocol.EncodeOpen(&conn.request, c.uri, 0, "volatile")

	if err := conn.protocol.Call(ctx, &conn.request, &conn.response); err != nil {
		conn.protocol.Close()
		return nil, driverError(conn.log, errors.Wrap(err, "failed to open database"))
	}

	conn.id, err = protocol.DecodeDb(&conn.response)
	if err != nil {
		conn.protocol.Close()
		return nil, driverError(conn.log, errors.Wrap(err, "failed to open database"))
	}

	return conn, nil
}

// Driver returns the underlying Driver of the Connector,
func (c *Connector) Driver() driver.Driver {
	return c.driver
}

// OpenConnector creates a reusable Connector for a specific database.
func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	config := d.clientConfig
	config.ConcurrentLeaderConns = *d.concurrentLeaderConns
	pc := protocol.NewLeaderConnector(d.store, config, d.log)
	connector := &Connector{
		uri:      name,
		driver:   d,
		protocol: pc,
	}
	return connector, nil
}

// Open establishes a new connection to a SQLite database on the dqlite server.
//
// The given name must be a pure file name without any directory segment,
// dqlite will connect to a database with that name in its data directory.
//
// Query parameters are always valid except for "mode=memory".
//
// If this node is not the leader, or the leader is unknown an ErrNotLeader
// error is returned.
func (d *Driver) Open(uri string) (driver.Conn, error) {
	connector, err := d.OpenConnector(uri)
	if err != nil {
		return nil, err
	}

	return connector.Connect(context.Background())
}

// SetContextTimeout sets the default client timeout when no context deadline
// is provided.
//
// DEPRECATED: This API is no a no-op. Users should explicitly pass a context
// if they wish to cancel their requests, or use the WithContextTimeout option.
func (d *Driver) SetContextTimeout(timeout time.Duration) {}

// ErrNoAvailableLeader is returned as root cause of Open() if there's no
// leader available in the cluster.
var ErrNoAvailableLeader = protocol.ErrNoAvailableLeader

// Conn implements the sql.Conn interface.
type Conn struct {
	log            client.LogFunc
	protocol       *protocol.Protocol
	request        protocol.Message
	response       protocol.Message
	id             uint32 // Database ID.
	contextTimeout time.Duration
	stmtCache      *stmtCache
	metrics        metrics.Recorder
}

// prepareOne asks SQLite to prepare the first statement in query. The returned
// offset is the exact statement boundary reported by SQLite.
func (c *Conn) prepareOne(ctx context.Context, query string) (_ *stmtRef, _ int, retErr error) {
	ctx, span := tracing.Start(ctx, "dqlite.driver.prepareOne", query)
	defer span.End()

	stmt := &Stmt{
		protocol: c.protocol,
		request:  &c.request,
		response: &c.response,
		log:      c.log,
		metrics:  c.metrics,
		sql:      query,
	}

	protocol.EncodePrepareV1(&c.request, uint64(c.id), query)

	if c.metrics != nil {
		metricStart := time.Now()
		defer func() {
			metrics.ObserveCommand(c.metrics, metrics.CommandPrepare, time.Since(metricStart), stmt.sql, retErr)
		}()
	}
	err := c.protocol.Call(ctx, &c.request, &c.response)
	if err != nil {
		return nil, 0, driverError(c.log, err)
	}

	var offset uint64
	stmt.db, stmt.id, stmt.params, offset, err = protocol.DecodeStmtWithOffset(&c.response)
	if err != nil {
		return nil, 0, driverError(c.log, err)
	}
	if offset == 0 || offset > uint64(len(query)) {
		stmt.finalize()
		return nil, 0, driverError(c.log, fmt.Errorf("invalid prepared statement offset %d for query of length %d", offset, len(query)))
	}

	stmt.sql = query[:offset]

	return &stmtRef{stmt: stmt}, int(offset), nil
}

// prepareNextStatement returns a lease for the first statement in query and
// the remaining SQL. It is the single place that handles separator trimming,
// cache lookup, preparation, and cache insertion. A nil lease means query
// contains no statement.
func (c *Conn) prepareNextStatement(ctx context.Context, query string) (*stmtLease, string, error) {
	query = trimSQLSeparators(query)
	if len(query) == 0 {
		return nil, "", nil
	}

	ref, offset := c.stmtCache.get(query)
	if ref == nil {
		result := metrics.CacheMiss
		if c.stmtCache.capacity <= 0 {
			result = metrics.CacheDisabled
		}
		metrics.ObserveCache(c.metrics, result)
		var err error
		ref, offset, err = c.prepareOne(ctx, query)
		if err != nil {
			return nil, "", err
		}

		tail := trimSQLSeparators(query[offset:])
		ref, err = c.stmtCache.put(query[:offset], len(tail) > 0, ref)
		if err != nil {
			return nil, "", err
		}
		return ref.acquire(), tail, nil
	}
	metrics.ObserveCache(c.metrics, metrics.CacheHit)

	return ref.acquire(), trimSQLSeparators(query[offset:]), nil
}

// PrepareContext returns one or more prepared statements, bound to this
// connection. Statement boundaries come from SQLite rather than a client-side
// SQL splitter, which is essential for semicolons in strings and comments.
func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	ctx, span := tracing.Start(ctx, "dqlite.driver.PrepareContext", query)
	defer span.End()

	var stmts []*stmtLease
	for {
		stmt, tail, err := c.prepareNextStatement(ctx, query)
		if err != nil {
			closeStmtLeases(stmts)
			return nil, err
		}
		if stmt == nil {
			break
		}
		stmts = append(stmts, stmt)
		query = tail
	}

	if len(stmts) == 0 {
		return emptyStmt{}, nil
	}
	if len(stmts) == 1 {
		return stmts[0], nil
	}
	return compoundStmt(stmts), nil
}

// Prepare returns a prepared statement, bound to this connection.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// ExecContext is an optional interface that may be implemented by a Conn.
func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	ctx, span := tracing.Start(ctx, "dqlite.driver.ExecContext", query)
	defer span.End()

	// Prepare and execute incrementally. Besides matching ExecSQL semantics,
	// this permits later statements to depend on earlier DDL in the same SQL
	// string (for example, CREATE TABLE followed by INSERT).
	var result driver.Result = &Result{}
	for {
		stmt, tail, err := c.prepareNextStatement(ctx, query)
		if err != nil {
			return nil, err
		}
		if stmt == nil {
			break
		}
		n := stmt.NumInput()
		if n > len(args) {
			stmt.Close()
			return nil, driverError(c.log, fmt.Errorf("bind parameters"))
		}
		result, err = stmt.ExecContext(ctx, args[:n])
		closeErr := stmt.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}

		args = args[n:]
		query = tail
	}
	if len(args) != 0 {
		return nil, driverError(c.log, fmt.Errorf("bind parameters"))
	}
	return result, nil
}

// Query is an optional interface that may be implemented by a Conn.
func (c *Conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return c.QueryContext(context.Background(), query, valuesToNamedValues(args))
}

// QueryContext is an optional interface that may be implemented by a Conn.
func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	ctx, span := tracing.Start(ctx, "dqlite.driver.QueryContext", query)
	defer span.End()

	stmt, err := c.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	if stmt.NumInput() != len(args) {
		return nil, driverError(c.log, fmt.Errorf("bind parameters"))
	}
	return stmt.(driver.StmtQueryContext).QueryContext(ctx, args)
}

// Exec is an optional interface that may be implemented by a Conn.
func (c *Conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	return c.ExecContext(context.Background(), query, valuesToNamedValues(args))
}

// Close invalidates and potentially stops any current prepared statements and
// transactions, marking this connection as no longer in use.
//
// Because the sql package maintains a free pool of connections and only calls
// Close when there's a surplus of idle connections, it shouldn't be necessary
// for drivers to do their own connection caching.
func (c *Conn) Close() error {
	cacheErr := c.stmtCache.close()
	protocolErr := c.protocol.Close()
	if cacheErr != nil {
		return cacheErr
	}
	return protocolErr
}

// BeginTx starts and returns a new transaction.  If the context is canceled by
// the user the sql package will call Tx.Rollback before discarding and closing
// the connection.
//
// This must check opts.Isolation to determine if there is a set isolation
// level. If the driver does not support a non-default level and one is set or
// if there is a non-default isolation level that is not supported, an error
// must be returned.
//
// This must also check opts.ReadOnly to determine if the read-only value is
// true to either set the read-only transaction property if supported or return
// an error if it is not supported.
func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if _, err := c.ExecContext(ctx, "BEGIN", nil); err != nil {
		return nil, err
	}

	tx := &Tx{
		conn: c,
		log:  c.log,
	}

	return tx, nil
}

// Begin starts and returns a new transaction.
//
// Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
func (c *Conn) Begin() (driver.Tx, error) {
	ctx := context.Background()

	if c.contextTimeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(context.Background(), c.contextTimeout)
		defer cancel()
	}

	return c.BeginTx(ctx, driver.TxOptions{})
}

// Tx is a transaction.
type Tx struct {
	conn *Conn
	log  client.LogFunc
}

// Commit the transaction.
func (tx *Tx) Commit() error {
	ctx := context.Background()

	if _, err := tx.conn.ExecContext(ctx, "COMMIT", nil); err != nil {
		return driverError(tx.log, err)
	}

	return nil
}

// Rollback the transaction.
func (tx *Tx) Rollback() error {
	ctx := context.Background()

	if _, err := tx.conn.ExecContext(ctx, "ROLLBACK", nil); err != nil {
		return driverError(tx.log, err)
	}

	return nil
}

// Stmt is a prepared statement. It is bound to a Conn and not
// used by multiple goroutines concurrently.
type Stmt struct {
	protocol  *protocol.Protocol
	request   *protocol.Message
	response  *protocol.Message
	db        uint32
	id        uint32
	params    uint64
	log       client.LogFunc
	sql       string
	metrics   metrics.Recorder
	finalized bool
}

func (s *Stmt) finalize() error {
	if s.finalized {
		return nil
	}
	s.finalized = true
	protocol.EncodeFinalize(s.request, s.db, s.id)

	ctx := context.Background()

	if err := s.protocol.Call(ctx, s.request, s.response); err != nil {
		return driverError(s.log, err)
	}

	if err := protocol.DecodeEmpty(s.response); err != nil {
		return driverError(s.log, err)
	}

	return nil
}

// Close closes an uncached physical statement. Cached statements are exposed
// to callers through stmtLease below.
func (s *Stmt) Close() error { return s.finalize() }

// NumInput returns the number of placeholder parameters.
func (s *Stmt) NumInput() int {
	return int(s.params)
}

// ExecContext executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// ExecContext must honor the context timeout and return when it is canceled.
func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (_ driver.Result, retErr error) {
	ctx, span := tracing.Start(ctx, "dqlite.driver.Stmt.ExecContext", s.sql)
	defer span.End()
	args = statementNamedValues(args)

	if int64(len(args)) > math.MaxUint32 {
		return nil, driverError(s.log, fmt.Errorf("too many parameters (%d)", len(args)))
	} else if len(args) > math.MaxUint8 {
		protocol.EncodeExecV1(s.request, s.db, s.id, args)
	} else {
		protocol.EncodeExecV0(s.request, s.db, s.id, args)
	}

	if s.metrics != nil {
		metricStart := time.Now()
		defer func() {
			metrics.ObserveCommand(s.metrics, metrics.CommandExec, time.Since(metricStart), s.sql, retErr)
		}()
	}
	err := s.protocol.Call(ctx, s.request, s.response)
	if err != nil {
		return nil, driverError(s.log, err)
	}

	var result protocol.Result
	result, err = protocol.DecodeResult(s.response)
	if err != nil {
		return nil, driverError(s.log, err)
	}

	return &Result{result: result}, nil
}

// Exec executes a query that doesn't return rows, such
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), valuesToNamedValues(args))
}

// QueryContext executes a query that may return rows, such as a
// SELECT.
//
// QueryContext must honor the context timeout and return when it is canceled.
func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.queryContext(ctx, args, nil)
}

func (s *Stmt) queryContext(ctx context.Context, args []driver.NamedValue, lease *stmtLease) (_ driver.Rows, retErr error) {
	ctx, span := tracing.Start(ctx, "dqlite.driver.Stmt.QueryContext", s.sql)
	defer span.End()

	args = statementNamedValues(args)

	if int64(len(args)) > math.MaxUint32 {
		return nil, driverError(s.log, fmt.Errorf("too many parameters (%d)", len(args)))
	} else if len(args) > math.MaxUint8 {
		protocol.EncodeQueryV1(s.request, s.db, s.id, args)
	} else {
		protocol.EncodeQueryV0(s.request, s.db, s.id, args)
	}

	if s.metrics != nil {
		metricStart := time.Now()
		defer func() {
			metrics.ObserveCommand(s.metrics, metrics.CommandQuery, time.Since(metricStart), s.sql, retErr)
		}()
	}
	err := s.protocol.Call(ctx, s.request, s.response)
	if err != nil {
		return nil, driverError(s.log, err)
	}

	var rows protocol.Rows
	rows, err = protocol.DecodeRows(s.response)
	if err != nil {
		return nil, driverError(s.log, err)
	}

	return &Rows{
		ctx:       ctx,
		request:   s.request,
		response:  s.response,
		protocol:  s.protocol,
		rows:      rows,
		log:       s.log,
		stmtLease: lease,
	}, nil
}

// Query executes a query that may return rows, such as a
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), valuesToNamedValues(args))
}

// stmtLease is a caller's reference to a cached physical statement. Closing a
// lease never finalizes a statement that is still cached or in use by Rows.
type stmtLease struct {
	ref    *stmtRef
	closed bool
}

var _ driver.Stmt = (*stmtLease)(nil)
var _ driver.StmtExecContext = (*stmtLease)(nil)
var _ driver.StmtQueryContext = (*stmtLease)(nil)

func (s *stmtLease) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.ref.release()
}

func (s *stmtLease) NumInput() int { return s.ref.stmt.NumInput() }

func (s *stmtLease) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.ref.stmt.ExecContext(ctx, args)
}

func (s *stmtLease) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), valuesToNamedValues(args))
}

func (s *stmtLease) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	rowsLease := s.ref.acquire()
	rows, err := s.ref.stmt.queryContext(ctx, args, rowsLease)
	if err != nil {
		rowsLease.Close()
		return nil, err
	}
	return rows, nil
}

func (s *stmtLease) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), valuesToNamedValues(args))
}

type compoundStmt []*stmtLease

var _ driver.Stmt = compoundStmt(nil)
var _ driver.StmtExecContext = compoundStmt(nil)
var _ driver.StmtQueryContext = compoundStmt(nil)

func closeStmtLeases(stmts []*stmtLease) error {
	var firstErr error
	for _, stmt := range stmts {
		if err := stmt.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s compoundStmt) Close() error { return closeStmtLeases(s) }

func (s compoundStmt) NumInput() int {
	total := 0
	for _, stmt := range s {
		total += stmt.NumInput()
	}
	return total
}

func (s compoundStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	// Conn.ExecContext and database/sql validate NumInput before reaching this
	// point, but keep the driver-level method panic-free for direct callers.
	if s.NumInput() != len(args) {
		return nil, fmt.Errorf("dqlite: expected %d arguments, got %d", s.NumInput(), len(args))
	}

	var result driver.Result = &Result{}
	for _, stmt := range s {
		n := stmt.NumInput()
		var err error
		result, err = stmt.ExecContext(ctx, args[:n])
		if err != nil {
			return nil, err
		}
		args = args[n:]
	}
	return result, nil
}

func (s compoundStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), valuesToNamedValues(args))
}

func (s compoundStmt) QueryContext(context.Context, []driver.NamedValue) (driver.Rows, error) {
	return nil, fmt.Errorf("dqlite: query contains multiple statements")
}

func (s compoundStmt) Query([]driver.Value) (driver.Rows, error) {
	return nil, fmt.Errorf("dqlite: query contains multiple statements")
}

// emptyStmt represents SQL containing only whitespace, separators, or
// comments. SQLite treats such input as a no-op, so it never reaches the
// server and owns no prepared-statement resources.
type emptyStmt struct{}

var _ driver.Stmt = emptyStmt{}
var _ driver.StmtExecContext = emptyStmt{}
var _ driver.StmtQueryContext = emptyStmt{}

func (emptyStmt) Close() error  { return nil }
func (emptyStmt) NumInput() int { return 0 }

func (emptyStmt) ExecContext(_ context.Context, args []driver.NamedValue) (driver.Result, error) {
	if len(args) != 0 {
		return nil, fmt.Errorf("bind parameters")
	}
	return &Result{}, nil
}

func (s emptyStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), valuesToNamedValues(args))
}

func (emptyStmt) QueryContext(_ context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) != 0 {
		return nil, fmt.Errorf("bind parameters")
	}
	return emptyRows{}, nil
}

func (s emptyStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), valuesToNamedValues(args))
}

type emptyRows struct{}

func (emptyRows) Columns() []string         { return nil }
func (emptyRows) Close() error              { return nil }
func (emptyRows) Next([]driver.Value) error { return io.EOF }

// Result is the result of a query execution.
type Result struct {
	result protocol.Result
}

// LastInsertId returns the database's auto-generated ID
// after, for example, an INSERT into a table with primary
// key.
func (r *Result) LastInsertId() (int64, error) {
	return int64(r.result.LastInsertID), nil
}

// RowsAffected returns the number of rows affected by the
// query.
func (r *Result) RowsAffected() (int64, error) {
	return int64(r.result.RowsAffected), nil
}

// Rows is an iterator over an executed query's results.
type Rows struct {
	ctx       context.Context
	protocol  *protocol.Protocol
	request   *protocol.Message
	response  *protocol.Message
	rows      protocol.Rows
	consumed  bool
	types     []string
	log       client.LogFunc
	stmtLease *stmtLease
	closed    bool
}

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty
// string should be returned for that entry.
func (r *Rows) Columns() []string {
	return r.rows.Columns
}

// Close closes the rows iterator.
func (r *Rows) Close() (err error) {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.stmtLease != nil {
		defer func() {
			if closeErr := r.stmtLease.Close(); err == nil {
				err = closeErr
			}
		}()
	}
	err = r.rows.Close()

	// If we consumed the whole result set, there's nothing to do as
	// there's no pending response from the server.
	if r.consumed {
		return nil
	}

	// If there is was a single-response result set, we're done.
	if err == io.EOF {
		return nil
	}

	// Let's issue an interrupt request and wait until we get an empty
	// response, signalling that the query was interrupted.
	if err := r.protocol.Interrupt(r.ctx, r.request, r.response); err != nil {
		return driverError(r.log, err)
	}

	return nil
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// Next should return io.EOF when there are no more rows.
func (r *Rows) Next(dest []driver.Value) error {
	err := r.rows.Next(dest)

	if err == protocol.ErrRowsPart {
		r.rows.Close()
		if err := r.protocol.More(r.ctx, r.response); err != nil {
			return driverError(r.log, err)
		}
		rows, err := protocol.DecodeRows(r.response)
		if err != nil {
			return driverError(r.log, err)
		}
		r.rows = rows
		return r.rows.Next(dest)
	}

	if err == io.EOF {
		r.consumed = true
	}

	return err
}

// ColumnTypeScanType implements RowsColumnTypeScanType.
func (r *Rows) ColumnTypeScanType(i int) reflect.Type {
	// column := sql.NewColumn(r.rows, i)

	// typ, err := r.protocol.ColumnTypeScanType(context.Background(), column)
	// if err != nil {
	// 	return nil
	// }

	// return typ.DriverType()
	return nil
}

// ColumnTypeDatabaseTypeName implements RowsColumnTypeDatabaseTypeName.
// warning: not thread safe
func (r *Rows) ColumnTypeDatabaseTypeName(i int) string {
	if r.types == nil {
		var err error
		r.types, err = r.rows.ColumnTypes()
		// an error might not matter if we get our types
		if err != nil && i >= len(r.types) {
			// a panic here doesn't really help,
			// as an empty column type is not the end of the world
			// but we should still inform the user of the failure
			const msg = "row (%p) error returning column #%d type: %v\n"
			r.log(client.LogWarn, msg, r, i, err)
			return ""
		}
	}
	return r.types[i]
}

// trimSQLSeparators removes only tokens that SQLite treats as trivia between
// statements. It is intentionally not a SQL splitter; statement boundaries
// still come exclusively from sqlite3_prepare_v2 via the protocol offset.
func trimSQLSeparators(query string) string {
	for len(query) > 0 {
		switch query[0] {
		case ' ', '\t', '\n', '\r', '\f', ';':
			query = query[1:]
			continue
		}

		if len(query) >= 2 && query[0] == '-' && query[1] == '-' {
			i := 2
			for i < len(query) && query[i] != '\n' {
				i++
			}
			query = query[i:]
			continue
		}
		if len(query) >= 2 && query[0] == '/' && query[1] == '*' {
			i := 2
			for i+1 < len(query) && (query[i] != '*' || query[i+1] != '/') {
				i++
			}
			if i+1 >= len(query) {
				return ""
			}
			query = query[i+2:]
			continue
		}
		return query
	}
	return ""
}

// Convert a driver.Value slice into a driver.NamedValue slice.
func valuesToNamedValues(args []driver.Value) []driver.NamedValue {
	namedValues := make([]driver.NamedValue, len(args))
	for i, value := range args {
		namedValues[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   value,
		}
	}
	return namedValues
}

// statementNamedValues returns values with the statement-local ordinals
// required by the wire encoding. Segments after the first statement in a
// compound query retain their original global ordinals and must be rebased.
func statementNamedValues(args []driver.NamedValue) []driver.NamedValue {
	for i := range args {
		if args[i].Ordinal != i+1 {
			values := append([]driver.NamedValue(nil), args...)
			for j := range values {
				values[j].Ordinal = j + 1
			}
			return values
		}
	}
	return args
}

type unwrappable interface {
	Unwrap() error
}

// TODO driver.ErrBadConn should not be returned when there's a possibility that
// the query has been executed. In our case there is a window in protocol.Call
// between `send` and `recv` where the send has succeeded but the recv has
// failed. In those cases we call driverError on the result of protocol.Call,
// possibly returning ErrBadCon.
// https://cs.opensource.google/go/go/+/refs/tags/go1.20.4:src/database/sql/driver/driver.go;drc=a32a592c8c14927c20ac42808e1fb2e55b2e9470;l=162
func driverError(log client.LogFunc, err error) error {
	switch err := errors.Cause(err).(type) {
	case syscall.Errno:
		log(client.LogDebug, "network connection lost: %v", err)
		return driver.ErrBadConn
	case *net.OpError:
		log(client.LogDebug, "network connection lost: %v", err)
		return driver.ErrBadConn
	case protocol.ErrRequest:
		switch err.Code {
		case errIoErrNotLeaderLegacy:
			fallthrough
		case errIoErrLeadershipLostLegacy:
			fallthrough
		case ErrIoErrNotLeader:
			fallthrough
		case ErrIoErrLeadershipLost:
			log(client.LogDebug, "leadership lost (%d - %s)", err.Code, err.Description)
			return driver.ErrBadConn
		case errNotFound:
			log(client.LogDebug, "not found - potentially after leadership loss (%d - %s)", err.Code, err.Description)
			return driver.ErrBadConn
		default:
			// FIXME: the server side sometimes return SQLITE_OK
			// even in case of errors. This issue is still being
			// investigated, but for now let's just mark this
			// connection as bad so the client will retry.
			if err.Code == 0 {
				log(client.LogWarn, "unexpected error code (%d - %s)", err.Code, err.Description)
				return driver.ErrBadConn
			}
			return Error{
				Code:    int(err.Code),
				Message: err.Description,
			}
		}
	default:
		// When using a TLS connection, the underlying error might get
		// wrapped by the stdlib itself with the new errors wrapping
		// conventions available since go 1.13. In that case we check
		// the underlying error with Unwrap() instead of Cause().
		if root, ok := err.(unwrappable); ok {
			err = root.Unwrap()
		}
		switch err.(type) {
		case *net.OpError:
			log(client.LogDebug, "network connection lost: %v", err)
			return driver.ErrBadConn
		}
	}
	if errors.Is(err, io.EOF) {
		log(client.LogDebug, "EOF detected: %v", err)
		return driver.ErrBadConn
	}
	return err
}
