package app

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/canonical/go-dqlite"
	"github.com/canonical/go-dqlite/client"
)

// Option can be used to tweak app parameters.
type Option func(*options)

// WithAddress sets the network address of the application node.
//
// Other application nodes must be able to connect to this application node
// using the given address.
//
// If the application node is not the first one in the cluster, the address
// must match the value that was passed to the App.Add() method upon
// registration.
//
// If not given the first non-loopback IP address of any of the system network
// interfaces will be used, with port 9000.
//
// The address must be stable across application restarts.
func WithAddress(address string) Option {
	return func(options *options) {
		options.Address = address
	}
}

// WithCluster must be used when starting a newly added application node for
// the first time.
//
// It should contain the addresses of one or more applications nodes which are
// already part of the cluster.
func WithCluster(cluster []string) Option {
	return func(options *options) {
		options.Cluster = cluster
	}
}

// WithExternalConn enables passing an external dial function that will be used
// whenever dqlite needs to make an outside connection.
//
// Also takes a net.Conn channel that should be received when the external connection has been accepted.
func WithExternalConn(dialFunc client.DialFunc, acceptCh chan net.Conn) Option {
	return func(options *options) {
		options.Conn = &connSetup{
			dialFunc: dialFunc,
			acceptCh: acceptCh,
		}
	}
}

// WithTLS enables TLS encryption of network traffic.
//
// The "listen" parameter must hold the TLS configuration to use when accepting
// incoming connections clients or application nodes.
//
// The "dial" parameter must hold the TLS configuration to use when
// establishing outgoing connections to other application nodes.
func WithTLS(listen *tls.Config, dial *tls.Config) Option {
	return func(options *options) {
		options.TLS = &tlsSetup{
			Listen: listen,
			Dial:   dial,
		}
	}
}

// WithUnixSocket allows setting a specific socket path for communication between go-dqlite and dqlite.
//
// The default is an empty string which means a random abstract unix socket.
func WithUnixSocket(path string) Option {
	return func(options *options) {
		options.UnixSocket = path
	}
}

// WithVoters sets the number of nodes in the cluster that should have the
// Voter role.
//
// When a new node is added to the cluster or it is started again after a
// shutdown it will be assigned the Voter role in case the current number of
// voters is below n.
//
// Similarly when a node with the Voter role is shutdown gracefully by calling
// the Handover() method, it will try to transfer its Voter role to another
// non-Voter node, if one is available.
//
// All App instances in a cluster must be created with the same WithVoters
// setting.
//
// The given value must be an odd number greater than one.
//
// The default value is 3.
func WithVoters(n int) Option {
	return func(options *options) {
		options.Voters = n
	}
}

// WithStandBys sets the number of nodes in the cluster that should have the
// StandBy role.
//
// When a new node is added to the cluster or it is started again after a
// shutdown it will be assigned the StandBy role in case there are already
// enough online voters, but the current number of stand-bys is below n.
//
// Similarly when a node with the StandBy role is shutdown gracefully by
// calling the Handover() method, it will try to transfer its StandBy role to
// another non-StandBy node, if one is available.
//
// All App instances in a cluster must be created with the same WithStandBys
// setting.
//
// The default value is 3.
func WithStandBys(n int) Option {
	return func(options *options) {
		options.StandBys = n
	}
}

// WithRolesAdjustmentFrequency sets the frequency at which the current cluster
// leader will check if the roles of the various nodes in the cluster matches
// the desired setup and perform promotions/demotions to adjust the situation
// if needed.
//
// The default is 30 seconds.
func WithRolesAdjustmentFrequency(frequency time.Duration) Option {
	return func(options *options) {
		options.RolesAdjustmentFrequency = frequency
	}
}

// WithLogFunc sets a custom log function.
func WithLogFunc(log client.LogFunc) Option {
	return func(options *options) {
		options.Log = log
	}
}

// WithFailureDomain sets the node's failure domain.
//
// Failure domains are taken into account when deciding which nodes to promote
// to Voter or StandBy when needed.
func WithFailureDomain(code uint64) Option {
	return func(options *options) {
		options.FailureDomain = code
	}
}

// WithNetworkLatency sets the average one-way network latency.
func WithNetworkLatency(latency time.Duration) Option {
	return func(options *options) {
		options.NetworkLatency = latency
	}
}

// WithSnapshotParams sets the raft snapshot parameters.
func WithSnapshotParams(params dqlite.SnapshotParams) Option {
	return func(options *options) {
		options.SnapshotParams = params
	}
}

// WithDiskMode enables or disables disk-mode.
// WARNING: This is experimental API, use with caution
// and prepare for data loss.
// UNSTABLE: Behavior can change in future.
// NOT RECOMMENDED for production use-cases, use at own risk.
func WithDiskMode(disk bool) Option {
	return func(options *options) {
		options.DiskMode = disk
	}
}

type tlsSetup struct {
	Listen *tls.Config
	Dial   *tls.Config
}

type connSetup struct {
	dialFunc client.DialFunc
	acceptCh chan net.Conn
}

type options struct {
	Address                  string
	Cluster                  []string
	Log                      client.LogFunc
	TLS                      *tlsSetup
	Conn                     *connSetup
	Voters                   int
	StandBys                 int
	RolesAdjustmentFrequency time.Duration
	FailureDomain            uint64
	NetworkLatency           time.Duration
	UnixSocket               string
	SnapshotParams           dqlite.SnapshotParams
	DiskMode                 bool
}

// Create a options object with sane defaults.
func defaultOptions() *options {
	return &options{
		Log:                      defaultLogFunc,
		Voters:                   3,
		StandBys:                 3,
		RolesAdjustmentFrequency: 30 * time.Second,
		DiskMode:                 false, // Be explicit about not enabling disk-mode by default.
	}
}

func isLoopback(iface *net.Interface) bool {
	return int(iface.Flags&net.FlagLoopback) > 0
}

// see https://stackoverflow.com/a/48519490/3613657
// Valid IPv4 notations:
//
//    "192.168.0.1": basic
//    "192.168.0.1:80": with port info
//
// Valid IPv6 notations:
//
//    "::FFFF:C0A8:1": basic
//    "::FFFF:C0A8:0001": leading zeros
//    "0000:0000:0000:0000:0000:FFFF:C0A8:1": double colon expanded
//    "::FFFF:C0A8:1%1": with zone info
//    "::FFFF:192.168.0.1": IPv4 literal
//    "[::FFFF:C0A8:1]:80": with port info
//    "[::FFFF:C0A8:1%1]:80": with zone and port info
func isIpV4(ip string) bool {
	return strings.Count(ip, ":") < 2
}

func defaultAddress() (addr string, err error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range ifaces {
		if isLoopback(&iface) {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		if len(addrs) == 0 {
			continue
		}
		addr, ok := addrs[0].(*net.IPNet)
		if !ok {
			continue
		}
		ipStr := addr.IP.String()
		if isIpV4(ipStr) {
			return addr.IP.String() + ":9000", nil
		} else {
			return "[" + addr.IP.String() + "]" + ":9000", nil
		}
	}

	return "", fmt.Errorf("no suitable net.Interface found: %v", err)
}

func defaultLogFunc(l client.LogLevel, format string, a ...interface{}) {
	// Log only error messages
	if l != client.LogError {
		return
	}
	msg := fmt.Sprintf("["+l.String()+"]"+" dqlite: "+format, a...)
	log.Printf(msg)
}
