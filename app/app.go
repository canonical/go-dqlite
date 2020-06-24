package app

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/canonical/go-dqlite"
	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/go-dqlite/driver"
	"github.com/pkg/errors"
)

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
	store           client.NodeStore
	driver          *driver.Driver
	driverName      string
	log             client.LogFunc
	stop            context.CancelFunc // Signal App.run() to stop.
	proxyCh         chan struct{}      // Waits for App.proxy() to return.
	runCh           chan struct{}      // Waits for App.run() to return.
	readyCh         chan struct{}      // Waits for startup tasks
	voters          int
	standbys        int
}

// New creates a new application node.
func New(dir string, options ...Option) (app *App, err error) {
	o := defaultOptions()
	for _, option := range options {
		option(o)
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
			o.Address = defaultAddress()
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
	var nodeBindAddress string
	var nodeDial client.DialFunc
	if o.TLS != nil {
		nodeBindAddress = fmt.Sprintf("@dqlite-%d", info.ID)

		// Within a snap we need to choose a different name for the abstract unix domain
		// socket to get it past the AppArmor confinement.
		// See https://github.com/snapcore/snapd/blob/master/interfaces/apparmor/template.go#L357
		snapInstanceName := os.Getenv("SNAP_INSTANCE_NAME")
		if len(snapInstanceName) > 0 {
			nodeBindAddress = fmt.Sprintf("@snap.%s.dqlite-%d", snapInstanceName, info.ID)
		}

		nodeDial = makeNodeDialFunc(o.TLS.Dial)
	} else {
		nodeBindAddress = info.Address
		nodeDial = client.DefaultDialFunc
	}
	node, err := dqlite.New(
		info.ID, info.Address, dir,
		dqlite.WithBindAddress(nodeBindAddress),
		dqlite.WithDialFunc(nodeDial),
	)
	if err != nil {
		return nil, fmt.Errorf("create node: %w", err)
	}
	if err := node.Start(); err != nil {
		return nil, fmt.Errorf("start node: %w", err)
	}
	cleanups = append(cleanups, func() { node.Close() })

	// Register the local dqlite driver.
	driverDial := client.DefaultDialFunc
	if o.TLS != nil {
		driverDial = client.DialFuncWithTLS(driverDial, o.TLS.Dial)
	}

	driver, err := driver.New(store, driver.WithDialFunc(driverDial), driver.WithLogFunc(o.Log))
	if err != nil {
		return nil, fmt.Errorf("create driver: %w", err)
	}
	driverIndex++
	driverName := fmt.Sprintf("dqlite-%d", driverIndex)
	sql.Register(driverName, driver)

	if o.Voters < 3 || o.Voters%2 == 0 {
		return nil, fmt.Errorf("invalid voters %d: must be an odd number greater than 1", o.Voters)
	}

	if o.StandBys < 0 || o.StandBys%2 != 0 {
		return nil, fmt.Errorf("invalid stand-bys %d: must be an even number greater than 0", o.StandBys)
	}

	ctx, stop := context.WithCancel(context.Background())

	app = &App{
		id:              info.ID,
		address:         info.Address,
		dir:             dir,
		node:            node,
		nodeBindAddress: nodeBindAddress,
		store:           store,
		driver:          driver,
		driverName:      driverName,
		log:             o.Log,
		tls:             o.TLS,
		stop:            stop,
		runCh:           make(chan struct{}, 0),
		readyCh:         make(chan struct{}, 0),
		voters:          o.Voters,
		standbys:        o.StandBys,
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

	}

	go app.run(ctx, o.RolesAdjustmentFrequency, joinFileExists)

	return app, nil
}

// Handover transfers all responsibilities for this node (such has leadership
// and voting rights) to another node, if one is available.
//
// This method should always be called before invoking Close(), in order to
// gracefully shutdown a node.
func (a *App) Handover(ctx context.Context) error {
	// Set a hard limit of one minute, in case the user-provided context
	// has no expiration. That avoids the call to hang forever in case a
	// majority of the cluster is down and no leader is available.
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()

	cli, err := a.Leader(ctx)
	if err != nil {
		return fmt.Errorf("find leader: %w", err)
	}
	defer cli.Close()

	// Check if we are the current leader and transfer leadership if so.
	leader, err := cli.Leader(ctx)
	if err != nil {
		return fmt.Errorf("leader address: %w", err)
	}

	if leader != nil && leader.Address == a.address {
		if err := cli.Transfer(ctx, 0); err != nil {
			return fmt.Errorf("transfer leadership: %w", err)
		}
		cli, err = a.Leader(ctx)
		if err != nil {
			return fmt.Errorf("find new leader: %w", err)
		}
		defer cli.Close()
	}

	// Possibly transfer our role.
	nodes, err := cli.Cluster(ctx)
	if err != nil {
		return fmt.Errorf("cluster servers: %w", err)
	}

	role := client.NodeRole(-1)
	for _, node := range nodes {
		if node.ID == a.id {
			role = node.Role
		}
	}

	// If we are not in the list, it means we were removed, just do
	// nothing.
	if role == -1 {
		return nil
	}

	// If we are a voter or a stand-by, let's transfer our role if
	// possible.
	if role == client.Voter || role == client.StandBy {
		index := a.probeNodes(nodes)

		// Spare nodes are always candidates.
		candidates := index[client.Spare][online]

		// Stand-by nodes are candidate if we need to transfer voting
		// rights, and they are preferred over spares.
		if role == client.Voter {
			candidates = append(index[client.StandBy][online], candidates...)
		}

		if len(candidates) == 0 {
			// No online node available to be promoted.
			return nil
		}

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

		// Demote ourselves.
		if err := cli.Assign(ctx, a.ID(), client.Spare); err != nil {
			return fmt.Errorf("demote ourselves: %w", err)
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
func (a *App) Leader(ctx context.Context) (*client.Client, error) {
	return client.FindLeader(ctx, a.store, a.clientOptions()...)
}

// Proxy incoming TLS connections.
func (a *App) proxy() {
	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
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
func (a *App) run(ctx context.Context, frequency time.Duration, join bool) {
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
				delay = frequency
				close(a.readyCh)
				cli.Close()
				continue
			}

			// If we are the leader, let's see if there's any
			// adjustment we should make to node roles.
			if err := a.maybeAdjustRoles(ctx, cli); err != nil {
				a.warn("adjust roles: %v", err)
			}
			cli.Close()
		}
	}
}

const minVoters = 3

// Possibly change our own role at startup.
func (a *App) maybePromoteOurselves(ctx context.Context, cli *client.Client, nodes []client.NodeInfo) error {
	// If the cluster is still to small, do nothing.
	if len(nodes) < minVoters {
		return nil
	}

	voters := 0
	standbys := 0
	role := client.NodeRole(-1)

	for _, node := range nodes {
		if node.ID == a.id {
			role = node.Role
		}
		switch node.Role {
		case client.Voter:
			voters++
		case client.StandBy:
			standbys++
		}
	}

	// If we are not in the list, it means we were removed, just do nothing.
	if role == -1 {
		return nil
	}

	// If we already have the Voter or StandBy role, there's nothing to do.
	if role == client.Voter || role == client.StandBy {
		return nil
	}

	// If we have already reached the desired number of voters and
	// stand-bys, there's nothing to do.
	if voters >= a.voters && standbys >= a.standbys {
		return nil
	}

	// Figure if we need to become stand-by or voter.
	role = client.StandBy
	if voters < a.voters {
		role = client.Voter
	}

	// Promote ourselves.
	if err := cli.Assign(ctx, a.id, role); err != nil {
		return fmt.Errorf("assign %s role to ourselves: %v", role, err)
	}

	// Possibly try to promote another node as well if we've reached the 3
	// node threshold. If we don't succeed in doing that, errors are
	// ignored since the leader will eventually notice that don't have
	// enough voters and will retry.
	if role == client.Voter && voters == 1 {
		for _, node := range nodes {
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

	if len(nodes) == 1 {
		return nil
	}

	// If the cluster is too small, make sure we have just one voter (us).
	if len(nodes) < minVoters {
		for _, node := range nodes {
			if node.ID == a.ID() || node.Role != client.Voter {
				continue
			}
			if err := cli.Assign(ctx, node.ID, client.Spare); err != nil {
				a.warn("demote %s from %s to spare: %v", node.Address, node.Role, err)
			}
		}
		return nil
	}

	index := a.probeNodes(nodes)

	// If we have exactly the desired number of voters and stand-bys, and they are all
	// online, we're good.
	if len(index[client.Voter][offline]) == 0 && len(index[client.Voter][online]) == a.voters && len(index[client.StandBy][offline]) == 0 && len(index[client.StandBy][online]) == a.standbys {
		return nil
	}

	// If we have less online voters than desired, let's try to promote
	// some other node.
	if n := len(index[client.Voter][online]); n < a.voters {
		candidates := index[client.StandBy][online]
		candidates = append(candidates, index[client.Spare][online]...)

		if len(candidates) == 0 {
			return nil
		}

		for i, node := range candidates {
			if err := cli.Assign(ctx, node.ID, client.Voter); err != nil {
				a.warn("promote %s from %s to voter: %v", node.Address, node.Role, err)
				if i == len(candidates)-1 {
					// We could not promote any node
					return fmt.Errorf("could not promote any online node to voter")
				}
				continue
			}
			a.debug("promoted %s from %s to voter", node.Address, node.Role)
			break
		}

		// Check again if we need more adjustments
		goto again
	}

	// If we have more online voters than desired, let's demote one of
	// them.
	if n := len(index[client.Voter][online]); n > a.voters {
		voters := index[client.Voter][online]
		for i, node := range voters {
			// Don't demote ourselves.
			if node.ID == a.id {
				continue
			}
			if err := cli.Assign(ctx, node.ID, client.Spare); err != nil {
				a.warn("demote online %s from voter to spare: %v", node.Address, err)
				if i == len(nodes)-1 {
					// We could not demote any node
					return fmt.Errorf("could not demote any redundant online voter")
				}
				continue
			}
			a.debug("demoted %s from voter to spare", node.Address)
			break
		}

		// Check again if we need more adjustments
		goto again
	}

	// If we have offline voters, let's demote one of them.
	if n := len(index[client.Voter][offline]); n > 0 {
		voters := index[client.Voter][offline]
		for i, node := range voters {
			if err := cli.Assign(ctx, node.ID, client.Spare); err != nil {
				a.warn("demote offline %s from voter to spare: %v", node.Address, err)
				if i == len(nodes)-1 {
					// We could not promote any node
					return fmt.Errorf("could not demote any offline voter node")
				}
				continue
			}
			a.debug("demoted offline voter %s", node.Address)
			break
		}

		// Check again if we need more adjustments
		goto again
	}

	// If we have less online stand-ys than desired, let's try to promote
	// some other node.
	if n := len(index[client.StandBy][online]); n < a.standbys {
		candidates := index[client.Spare][online]

		if len(candidates) == 0 {
			return nil
		}

		for i, node := range candidates {
			if err := cli.Assign(ctx, node.ID, client.StandBy); err != nil {
				a.warn("promote %s to stand-by: %v", node.Address, err)
				if i == len(candidates)-1 {
					// We could not promote any node
					return fmt.Errorf("could not promote any online node to stand-by")
				}
				continue
			}
			a.debug("promoted %s to stand-by", node.Address)
			break
		}

		// Check again if we need more adjustments
		goto again
	}

	// If we have more online stand-bys than desired, let's demote one of
	// them.
	if n := len(index[client.StandBy][online]); n > a.standbys {
		standbys := index[client.StandBy][online]
		for i, node := range standbys {
			// Don't demote ourselves.
			if node.ID == a.id {
				continue
			}
			if err := cli.Assign(ctx, node.ID, client.Spare); err != nil {
				a.warn("demote online %s from stand-by to spare: %v", node.Address, err)
				if i == len(nodes)-1 {
					// We could not demote any node
					return fmt.Errorf("could not demote any redundant online stand-by")
				}
				continue
			}
			a.debug("demoted %s from stand-by to spare", node.Address)
			break
		}

		// Check again if we need more adjustments
		goto again
	}

	// If we have offline stand-bys, let's demote one of them.
	if n := len(index[client.StandBy][offline]); n > 0 {
		standbys := index[client.StandBy][offline]
		for i, node := range standbys {
			if err := cli.Assign(ctx, node.ID, client.Spare); err != nil {
				a.warn("demote offline %s from stand-by to spare: %v", node.Address, err)
				if i == len(nodes)-1 {
					// We could not promote any node
					return fmt.Errorf("could not demote any offline stand-by node")
				}
				continue
			}
			a.debug("demoted offline stand-y %s", node.Address)
			break
		}

		// Check again if we need more adjustments
		goto again
	}

	return nil
}

const (
	online  = 0
	offline = 1
)

// Probe all given nodes for connectivity, grouping them by role and by
// online/offline state.
func (a *App) probeNodes(nodes []client.NodeInfo) map[client.NodeRole][2][]client.NodeInfo {
	// Group all nodes by role, and divide them between online an not
	// online.
	index := map[client.NodeRole][2][]client.NodeInfo{
		client.Spare:   {{}, {}},
		client.StandBy: {{}, {}},
		client.Voter:   {{}, {}},
	}
	for _, node := range nodes {
		state := offline
		if node.ID == a.id {
			state = online
		} else {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			cli, err := client.New(ctx, node.Address, a.clientOptions()...)
			if err == nil {
				state = online
				cli.Close()
			}
		}
		role := index[node.Role]
		role[state] = append(role[state], node)
		index[node.Role] = role
	}

	return index
}

// Return the options to use for client.FindLeader() or client.New()
func (a *App) clientOptions() []client.Option {
	dial := client.DefaultDialFunc
	if a.tls != nil {
		dial = client.DialFuncWithTLS(dial, a.tls.Dial)
	}
	return []client.Option{client.WithDialFunc(dial), client.WithLogFunc(a.log)}
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

var driverIndex = 0
