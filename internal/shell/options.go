package shell

import "github.com/canonical/go-dqlite/client"

// Option that can be used to tweak shell parameters.
type Option func(*options)

// WithDialFunc sets a custom dial function for connecting to dqlite endpoints.
func WithDialFunc(dial client.DialFunc) Option {
	return func(options *options) {
		options.Dial = dial
	}
}

// WithDriverName sets a custom name for the registered dqlite driver. The
// default is "dqlite".
func WithDriverName(name string) Option {
	return func(options *options) {
		options.DriverName = name
	}
}

// WithFormat specifies the output format.
func WithFormat(format string) Option {
	return func(options *options) {
		options.Format = format
	}
}

type options struct {
	Dial       client.DialFunc
	DriverName string
	Format     string
}

// Create a client options object with sane defaults.
func defaultOptions() *options {
	return &options{
		Dial:       client.DefaultDialFunc,
		DriverName: "dqlite",
		Format:     formatTabular,
	}
}

const (
	formatTabular = "tabular"
	formatJson    = "json"
)
