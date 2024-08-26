package app

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/canonical/go-dqlite"
	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/go-dqlite/driver"
	"github.com/canonical/go-dqlite/internal/protocol"
	"github.com/pkg/errors"
	"golang.org/x/sync/semaphore"
)

// used to create a unique driver name, MUST be modified atomically
// https://pkg.go.dev/sync/atomic#AddInt64
var driverIndex int64

// App is a high-level helper for initializing a typical dqlite-based Go
// application.
//
// It takes care of starting a dqlite node and registering a dqlite Go SQL
// driver.
type App struct {
	id              uint64
	address         string
	dir             string
	node            *dqlite.Node
	nodeBindAddress string
	listener        net.Listener
	tls             *tlsSetup
	dialFunc        client.DialFunc
	store           client.NodeStore
	driver          *driver.Driver
	driverName      string
	log             client.LogFunc
	ctx             context.Context
	stop            context.CancelFunc // Signal App.run() to stop.
	proxyCh         chan struct{}      // Waits for App.proxy() to return.
	runCh           chan struct{}      // Waits for App.run() to return.
	readyCh         chan struct{}      // Waits for startup tasks
	voters          int
	standbys        int
	roles           RolesConfig
	options         *options
}

// New creates a new application node.
func New(dir string, options ...Option) (app *App, err error) {
	o := defaultOptions()
	for _, option := range options {
		option(o)
	}

	var nodeBindAddress string
	if o.Conn != nil {
		listener, err := net.Listen("unix", o.UnixSocket)
		if err != nil {
			return nil, fmt.Errorf("failed to autobind unix socket: %w", err)
		}

		nodeBindAddress = listener.Addr().String()
		listener.Close()
	}

	// List of cleanup functions to run in case of errors.
	cleanups := []func(){}
	defer func() {
		if err == nil {
			return
		}
		for i := range cleanups {
			i = len(cleanups) - 1 - i // Reverse order
			cleanups[i]()
		}
	}()

	// Load our ID, or generate one if we are joining.
	info := client.NodeInfo{}
	infoFileExists, err := fileExists(dir, infoFile)
	if err != nil {
		return nil, err
	}
	if !infoFileExists {
		if o.Address == "" {
			if o.Address, err = defaultAddress(); err != nil {
				return nil, err
			}
		}
		if len(o.Cluster) == 0 {
			info.ID = dqlite.BootstrapID
		} else {
			info.ID = dqlite.GenerateID(o.Address)
			if err := fileWrite(dir, joinFile, []byte{}); err != nil {
				return nil, err
			}
		}
		info.Address = o.Address

		if err := fileMarshal(dir, infoFile, info); err != nil {
			return nil, err
		}

		cleanups = append(cleanups, func() { fileRemove(dir, infoFile) })
	} else {
		if err := fileUnmarshal(dir, infoFile, &info); err != nil {
			return nil, err
		}
		if o.Address != "" && o.Address != info.Address {
			return nil, fmt.Errorf("address %q in info.yaml does not match %q", info.Address, o.Address)
		}
	}

	joinFileExists, err := fileExists(dir, joinFile)
	if err != nil {
		return nil, err
	}

	if info.ID == dqlite.BootstrapID && joinFileExists {
		return nil, fmt.Errorf("bootstrap node can't join a cluster")
	}

	// Open the nodes store.
	storeFileExists, err := fileExists(dir, storeFile)
	if err != nil {
		return nil, err
	}
	store, err := client.NewYamlNodeStore(filepath.Join(dir, storeFile))
	if err != nil {
		return nil, fmt.Errorf("open cluster.yaml node store: %w", err)
	}

	// The info file and the store file should both exists or none of them
	// exist.
	if infoFileExists != storeFileExists {
		return nil, fmt.Errorf("inconsistent info.yaml and cluster.yaml")
	}

	if !storeFileExists {
		// If this is a brand new application node, populate the store
		// either with the node's address (for bootstrap nodes) or with
		// the given cluster addresses (for joining nodes).
		nodes := []client.NodeInfo{}
		if info.ID == dqlite.BootstrapID {
			nodes = append(nodes, client.NodeInfo{Address: info.Address})
		} else {
			if len(o.Cluster) == 0 {
				return nil, fmt.Errorf("no cluster addresses provided")
			}
			for _, address := range o.Cluster {
				nodes = append(nodes, client.NodeInfo{Address: address})
			}
		}
		if err := store.Set(context.Background(), nodes); err != nil {
			return nil, fmt.Errorf("initialize node store: %w", err)
		}
		cleanups = append(cleanups, func() { fileRemove(dir, storeFile) })
	}

	// Start the local dqlite engine.
	ctx, stop := context.WithCancel(context.Background())
	var nodeDial client.DialFunc
	if o.Conn != nil {
		nodeDial = extDialFuncWithProxy(ctx, o.Conn.dialFunc)
	} else if o.TLS != nil {
		nodeBindAddress = fmt.Sprintf("@dqlite-%d", info.ID)

		// Within a snap we need to choose a different name for the abstract unix domain
		// socket to get it past the AppArmor confinement.
		// See https://github.com/snapcore/snapd/blob/master/interfaces/apparmor/template.go#L357
		snapInstanceName := os.Getenv("SNAP_INSTANCE_NAME")
		if len(snapInstanceName) > 0 {
			nodeBindAddress = fmt.Sprintf("@snap.%s.dqlite-%d", snapInstanceName, info.ID)
		}

		nodeDial = makeNodeDialFunc(ctx, o.TLS.Dial)
	} else {
		nodeBindAddress = info.Address
		nodeDial = client.DefaultDialFunc
	}
	node, err := dqlite.New(
		info.ID, info.Address, dir,
		dqlite.WithBindAddress(nodeBindAddress),
		dqlite.WithDialFunc(nodeDial),
		dqlite.WithFailureDomain(o.FailureDomain),
		dqlite.WithNetworkLatency(o.NetworkLatency),
		dqlite.WithSnapshotParams(o.SnapshotParams),
		dqlite.WithDiskMode(o.DiskMode),
		dqlite.WithAutoRecovery(o.AutoRecovery),
	)
	if err != nil {
		stop()
		return nil, fmt.Errorf("create node: %w", err)
	}
	if err := node.Start(); err != nil {
		stop()
		return nil, fmt.Errorf("start node: %w", err)
	}
	cleanups = append(cleanups, func() { node.Close() })

	// Register the local dqlite driver.
	driverDial := client.DefaultDialFunc
	if o.TLS != nil {
		driverDial = client.DialFuncWithTLS(driverDial, o.TLS.Dial)
	} else if o.Conn != nil {
		driverDial = o.Conn.dialFunc
	}

	driver, err := driver.New(
		store,
		driver.WithDialFunc(driverDial),
		driver.WithLogFunc(o.Log),
		driver.WithTracing(o.Tracing),
		driver.WithConcurrentLeaderConns(o.ConcurrentLeaderConns),
	)
	if err != nil {
		stop()
		return nil, fmt.Errorf("create driver: %w", err)
	}
	driverName := fmt.Sprintf("dqlite-%d", atomic.AddInt64(&driverIndex, 1))
	sql.Register(driverName, driver)

	if o.Voters < 3 || o.Voters%2 == 0 {
		stop()
		return nil, fmt.Errorf("invalid voters %d: must be an odd number greater than 1", o.Voters)
	}

	if runtime.GOOS != "linux" && nodeBindAddress[0] == '@' {
		// Do not use abstract socket on other platforms and left trim "@"
		nodeBindAddress = nodeBindAddress[1:]
	}

	app = &App{
		id:              info.ID,
		address:         info.Address,
		dir:             dir,
		node:            node,
		nodeBindAddress: nodeBindAddress,
		store:           store,
		dialFunc:        driverDial,
		driver:          driver,
		driverName:      driverName,
		log:             o.Log,
		tls:             o.TLS,
		ctx:             ctx,
		stop:            stop,
		runCh:           make(chan struct{}, 0),
		readyCh:         make(chan struct{}, 0),
		voters:          o.Voters,
		standbys:        o.StandBys,
		roles:           RolesConfig{Voters: o.Voters, StandBys: o.StandBys},
		options:         o,
	}

	// Start the proxy if a TLS configuration was provided.
	if o.TLS != nil {
		listener, err := net.Listen("tcp", info.Address)
		if err != nil {
			return nil, fmt.Errorf("listen to %s: %w", info.Address, err)
		}
		proxyCh := make(chan struct{}, 0)

		app.listener = listener
		app.proxyCh = proxyCh

		go app.proxy()

		cleanups = append(cleanups, func() { listener.Close(); <-proxyCh })

	} else if o.Conn != nil {
		go func() {
			for remote := range o.Conn.acceptCh {

				// keep forward compatible
				_, isTcp := remote.(*net.TCPConn)
				_, isTLS := remote.(*tls.Conn)

				if isTcp || isTLS {
					// Write the status line and upgrade header by hand since w.WriteHeader() would fail after Hijack().
					data := []byte("HTTP/1.1 101 Switching Protocols\r\nUpgrade: dqlite\r\n\r\n")
					n, err := remote.Write(data)
					if err != nil || n != len(data) {
						remote.Close()
						panic(fmt.Errorf("failed to write connection header: %w", err))
					}
				}

				local, err := net.Dial("unix", nodeBindAddress)
				if err != nil {
					remote.Close()
					panic(fmt.Errorf("failed to connect to bind address %q: %w", nodeBindAddress, err))
				}

				go proxy(app.ctx, remote, local, nil)
			}
		}()
	}

	go app.run(ctx, o, joinFileExists)

	return app, nil
}

// Handover transfers all responsibilities for this node (such has leadership
// and voting rights) to another node, if one is available.
//
// This method should always be called before invoking Close(), in order to
// gracefully shutdown a node.
func (a *App) Handover(ctx context.Context) error {
	// Set a hard limit of one minute, in case the user-provided context
	// has no expiration. That avoids the call to stop responding forever
	// in case a majority of the cluster is down and no leader is available.
	// Watch out when removing or editing this context, the for loop at the
	// end of this function will possibly run "forever" without it.
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()

	cli, err := a.Leader(ctx)
	if err != nil {
		return fmt.Errorf("find leader: %w", err)
	}
	defer cli.Close()

	// Possibly transfer our role.
	nodes, err := cli.Cluster(ctx)
	if err != nil {
		return fmt.Errorf("cluster servers: %w", err)
	}

	changes := a.makeRolesChanges(nodes)

	role, candidates := changes.Handover(a.id)

	if role != -1 {
		for i, node := range candidates {
			if err := cli.Assign(ctx, node.ID, role); err != nil {
				a.warn("promote %s from %s to %s: %v", node.Address, node.Role, role, err)
				if i == len(candidates)-1 {
					// We could not promote any node
					return fmt.Errorf("could not promote any online node to %s", role)
				}
				continue
			}
			a.debug("promoted %s from %s to %s", node.Address, node.Role, role)
			break
		}
	}

	// Check if we are the current leader and transfer leadership if so.
	leader, err := cli.Leader(ctx)
	if err != nil {
		return fmt.Errorf("leader address: %w", err)
	}
	if leader != nil && leader.Address == a.address {
		nodes, err := cli.Cluster(ctx)
		if err != nil {
			return fmt.Errorf("cluster servers: %w", err)
		}
		changes := a.makeRolesChanges(nodes)
		voters := changes.list(client.Voter, true, nil)

		for i, voter := range voters {
			if voter.Address == a.address {
				continue
			}
			if err := cli.Transfer(ctx, voter.ID); err != nil {
				a.warn("transfer leadership to %s: %v", voter.Address, err)
				if i == len(voters)-1 {
					return fmt.Errorf("transfer leadership: %w", err)
				}
			}
			cli, err = a.Leader(ctx)
			if err != nil {
				return fmt.Errorf("find new leader: %w", err)
			}
			defer cli.Close()
			break
		}
	}

	// Demote ourselves if we have promoted someone else.
	if role != -1 {
		// Try a while before failing. The new leader has to possibly commit an entry
		// from its new term in order to commit the last configuration change, wait a bit
		// for that to happen and don't fail immediately
		for {
			err = cli.Assign(ctx, a.ID(), client.Spare)
			if err == nil {
				return nil
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("demote ourselves context done: %w", err)
			default:
				// Wait a bit before trying again
				time.Sleep(time.Second)
				continue
			}
		}
	}

	return nil
}

// Close the application node, releasing all resources it created.
func (a *App) Close() error {
	// Stop the run goroutine.
	a.stop()
	<-a.runCh

	if a.listener != nil {
		a.listener.Close()
		<-a.proxyCh
	}
	if err := a.node.Close(); err != nil {
		return err
	}
	return nil
}

// ID returns the dqlite ID of this application node.
func (a *App) ID() uint64 {
	return a.id
}

// Address returns the dqlite address of this application node.
func (a *App) Address() string {
	return a.address
}

// Driver returns the name used to register the dqlite driver.
func (a *App) Driver() string {
	return a.driverName
}

// Ready can be used to wait for a node to complete some initial tasks that are
// initiated at startup. For example a brand new node will attempt to join the
// cluster, a restarted node will check if it should assume some particular
// role, etc.
//
// If this method returns without error it means that those initial tasks have
// succeeded and follow-up operations like Open() are more likely to succeeed
// quickly.
func (a *App) Ready(ctx context.Context) error {
	select {
	case <-a.readyCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Open the dqlite database with the given name
func (a *App) Open(ctx context.Context, database string) (*sql.DB, error) {
	db, err := sql.Open(a.Driver(), database)
	if err != nil {
		return nil, err
	}

	for i := 0; i < 60; i++ {
		err = db.PingContext(ctx)
		if err == nil {
			break
		}
		cause := errors.Cause(err)
		if cause != driver.ErrNoAvailableLeader {
			return nil, err
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		return nil, err
	}

	return db, nil
}

// Leader returns a client connected to the current cluster leader, if any.
func (a *App) Leader(ctx context.Context, options ...client.Option) (*client.Client, error) {
	allOptions := a.clientOptions()
	allOptions = append(allOptions, options...)

	return client.FindLeader(ctx, a.store, allOptions...)
}

// Client returns a client connected to the local node.
func (a *App) Client(ctx context.Context) (*client.Client, error) {
	return client.New(ctx, a.nodeBindAddress)
}

// Proxy incoming TLS connections.
func (a *App) proxy() {
	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(a.ctx)
	for {
		client, err := a.listener.Accept()
		if err != nil {
			cancel()
			wg.Wait()
			close(a.proxyCh)
			return
		}
		address := client.RemoteAddr()
		a.debug("new connection from %s", address)
		server, err := net.Dial("unix", a.nodeBindAddress)
		if err != nil {
			a.error("dial local node: %v", err)
			client.Close()
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := proxy(ctx, client, server, a.tls.Listen); err != nil {
				a.error("proxy: %v", err)
			}
		}()
	}
}

// Run background tasks. The join flag is true if the node is a brand new one
// and should join the cluster.
func (a *App) run(ctx context.Context, options *options, join bool) {
	defer close(a.runCh)

	delay := time.Duration(0)
	ready := false
	for {
		select {
		case <-ctx.Done():
			// If we didn't become ready yet, close the ready
			// channel, to unblock any call to Ready().
			if !ready {
				close(a.readyCh)
			}
			return
		case <-time.After(delay):
			cli, err := a.Leader(ctx)
			if err != nil {
				continue
			}

			// Attempt to join the cluster if this is a brand new node.
			if join {
				info := client.NodeInfo{ID: a.id, Address: a.address, Role: client.Spare}
				if err := cli.Add(ctx, info); err != nil {
					a.warn("join cluster: %v", err)
					delay = time.Second
					cli.Close()
					continue
				}
				join = false
				if err := fileRemove(a.dir, joinFile); err != nil {
					a.error("remove join file: %v", err)
				}

			}

			// Refresh our node store.
			servers, err := cli.Cluster(ctx)
			if err != nil {
				cli.Close()
				continue
			}
			if len(servers) == 0 {
				a.warn("server list empty")
				cli.Close()
				continue
			}
			a.store.Set(ctx, servers)

			// If we are starting up, let's see if we should
			// promote ourselves.
			if !ready {
				if err := a.maybePromoteOurselves(ctx, cli, servers); err != nil {
					a.warn("%v", err)
					delay = time.Second
					cli.Close()
					continue
				}
				ready = true
				delay = options.RolesAdjustmentFrequency
				close(a.readyCh)
				cli.Close()
				continue
			}

			// If we are the leader, let's see if there's any
			// adjustment we should make to node roles.
			if err := a.maybeAdjustRoles(ctx, cli); err != nil {
				a.warn("adjust roles: %v", err)
			}

			leader, err := cli.Leader(ctx)
			if err != nil {
				a.error("fetch leader info: %v", err)
				cli.Close()
				continue
			}

			err = options.OnRolesAdjustment(*leader, servers)
			if err != nil {
				a.warn("roles adjustment hook: %v", err)
			}

			cli.Close()
		}
	}
}

// Possibly change our own role at startup.
func (a *App) maybePromoteOurselves(ctx context.Context, cli *client.Client, nodes []client.NodeInfo) error {
	roles := a.makeRolesChanges(nodes)

	role := roles.Assume(a.id)
	if role == -1 {
		return nil
	}

	// Promote ourselves.
	if err := cli.Assign(ctx, a.id, role); err != nil {
		return fmt.Errorf("assign %s role to ourselves: %v", role, err)
	}

	// Possibly try to promote another node as well if we've reached the 3
	// node threshold. If we don't succeed in doing that, errors are
	// ignored since the leader will eventually notice that don't have
	// enough voters and will retry.
	if role == client.Voter && roles.count(client.Voter, true) == 1 {
		for node := range roles.State {
			if node.ID == a.id || node.Role == client.Voter {
				continue
			}
			if err := cli.Assign(ctx, node.ID, client.Voter); err == nil {
				break
			} else {
				a.warn("promote %s from %s to voter: %v", node.Address, node.Role, err)
			}
		}
	}

	return nil
}

// Check if any adjustment needs to be made to existing roles.
func (a *App) maybeAdjustRoles(ctx context.Context, cli *client.Client) error {
again:
	info, err := cli.Leader(ctx)
	if err != nil {
		return err
	}
	if info.ID != a.id {
		return nil
	}

	nodes, err := cli.Cluster(ctx)
	if err != nil {
		return err
	}

	roles := a.makeRolesChanges(nodes)

	role, nodes := roles.Adjust(a.id)
	if role == -1 {
		return nil
	}

	for i, node := range nodes {
		if err := cli.Assign(ctx, node.ID, role); err != nil {
			a.warn("change %s from %s to %s: %v", node.Address, node.Role, role, err)
			if i == len(nodes)-1 {
				// We could not change any node
				return fmt.Errorf("could not assign role %s to any node", role)
			}
			continue
		}
		break
	}

	goto again
}

// Probe all given nodes for connectivity and metadata, then return a
// RolesChanges object.
func (a *App) makeRolesChanges(nodes []client.NodeInfo) RolesChanges {
	state := map[client.NodeInfo]*client.NodeMetadata{}
	for _, node := range nodes {
		state[node] = nil
	}

	var (
		mtx     sync.Mutex     // Protects state map
		wg      sync.WaitGroup // Wait for all probes to finish
		nProbes = runtime.NumCPU()
		sem     = semaphore.NewWeighted(int64(nProbes)) // Limit number of parallel probes
	)

	for _, node := range nodes {
		wg.Add(1)
		// sem.Acquire will not block forever because the goroutines
		// that release the semaphore will eventually timeout.
		if err := sem.Acquire(context.Background(), 1); err != nil {
			a.warn("failed to acquire semaphore: %v", err)
			wg.Done()
			continue
		}
		go func(node protocol.NodeInfo) {
			defer wg.Done()
			defer sem.Release(1)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			cli, err := client.New(ctx, node.Address, a.clientOptions()...)
			if err == nil {
				metadata, err := cli.Describe(ctx)
				if err == nil {
					mtx.Lock()
					state[node] = metadata
					mtx.Unlock()
				}
				cli.Close()
			}
		}(node)
	}

	wg.Wait()
	return RolesChanges{Config: a.roles, State: state}
}

// Return the options to use for client.FindLeader() or client.New()
func (a *App) clientOptions() []client.Option {
	return []client.Option{client.WithDialFunc(a.dialFunc), client.WithLogFunc(a.log), client.WithConcurrentLeaderConns(*a.options.ConcurrentLeaderConns)}
}

func (a *App) debug(format string, args ...interface{}) {
	a.log(client.LogDebug, format, args...)
}

func (a *App) info(format string, args ...interface{}) {
	a.log(client.LogInfo, format, args...)
}

func (a *App) warn(format string, args ...interface{}) {
	a.log(client.LogWarn, format, args...)
}

func (a *App) error(format string, args ...interface{}) {
	a.log(client.LogError, format, args...)
}
