package app

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"

	"github.com/canonical/go-dqlite/client"
	"github.com/lxc/lxd/shared"
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

// WithLogFunc sets a custom log function.
func WithLogFunc(log client.LogFunc) Option {
	return func(options *options) {
		options.Log = log
	}
}

type tlsSetup struct {
	Listen *tls.Config
	Dial   *tls.Config
}

type options struct {
	Address string
	Cluster []string
	Dial    client.DialFunc
	Log     client.LogFunc
	TLS     *tlsSetup
}

// Create a options object with sane defaults.
func defaultOptions() *options {
	return &options{
		Address: defaultAddress(),
		Dial:    client.DefaultDialFunc,
		Log:     defaultLogFunc,
	}
}

func defaultAddress() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return ""
	}
	for _, iface := range ifaces {
		if shared.IsLoopback(&iface) {
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
