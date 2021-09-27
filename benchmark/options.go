package benchmark

import (
	"strings"
	"time"
)

type workload int32

const (
	kvWrite     workload = iota
	kvReadWrite workload = iota
)

type Option func(*options)
type options struct {
	cluster        []string
	clusterTimeout time.Duration
	workload       workload
	duration       time.Duration
	nWorkers       int
	kvKeySizeB     int
	kvValueSizeB   int
}

func parseWorkload(workload string) workload {
	switch strings.ToLower(workload) {
	case "kvwrite":
		return kvWrite
	case "kvreadwrite":
		return kvReadWrite
	default:
		return kvWrite
	}
}

// WithWorkload sets the workload of the benchmark.
func WithWorkload(workload string) Option {
	return func(options *options) {
		options.workload = parseWorkload(workload)
	}
}

// WithDuration sets the duration of the benchmark.
func WithDuration(seconds int) Option {
	return func(options *options) {
		options.duration = time.Duration(seconds) * time.Second
	}
}

// WithWorkers sets the number of workers of the benchmark.
func WithWorkers(n int) Option {
	return func(options *options) {
		options.nWorkers = n
	}
}

// WithKvKeySize sets the size of the KV keys of the benchmark.
func WithKvKeySize(bytes int) Option {
	return func(options *options) {
		options.kvKeySizeB = bytes
	}
}

// WithKvValueSize sets the size of the KV values of the benchmark.
func WithKvValueSize(bytes int) Option {
	return func(options *options) {
		options.kvValueSizeB = bytes
	}
}

// WithCluster sets the cluster option of the benchmark. A benchmark will only
// start once the whole cluster is online.
func WithCluster(cluster []string) Option {
	return func(options *options) {
		options.cluster = cluster
	}
}

// WithClusterTimeout sets the timeout when waiting for the whole cluster to be
// online
func WithClusterTimeout(cTo int) Option {
	return func(options *options) {
		options.clusterTimeout = time.Duration(cTo) * time.Second
	}
}

func defaultOptions() *options {
	return &options{
		cluster:        nil,
		clusterTimeout: time.Minute,
		duration:       time.Minute,
		kvKeySizeB:     32,
		kvValueSizeB:   1024,
		nWorkers:       1,
		workload:       kvWrite,
	}
}
