package app

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"time"

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
// The given value must be an odd number.
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

type tlsSetup struct {
	Listen *tls.Config
	Dial   *tls.Config
}

type options struct {
	Address                  string
	Cluster                  []string
	Log                      client.LogFunc
	TLS                      *tlsSetup
	Voters                   int
	StandBys                 int
	RolesAdjustmentFrequency time.Duration
	FailureDomain            uint64
	NetworkLatency           time.Duration
}

// Create a options object with sane defaults.
func defaultOptions() *options {
	return &options{
		Log:                      defaultLogFunc,
		Voters:                   3,
		StandBys:                 3,
		RolesAdjustmentFrequency: 30 * time.Second,
	}
}

func isLoopback(iface *net.Interface) bool {
	return int(iface.Flags&net.FlagLoopback) > 0
}

func defaultAddress() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return ""
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
		return addr.IP.String() + ":9000"
	}
	return ""
}

func defaultLogFunc(l client.LogLevel, format string, a ...interface{}) {
	// Log only error messages
	if l != client.LogError {
		return
	}
	msg := fmt.Sprintf("["+l.String()+"]"+" dqlite: "+format, a...)
	log.Printf(msg)
}
