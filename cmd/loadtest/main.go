package main

// This command will execute a series of inserts into a
// fresh database table ('simple'), comprising:
//
// id:    integer primary key (auto-incrementing)
// other: integer that should mirror the id field
// ts:    text, a text timestamp with microsecond resolution
//
// It is intendend to put go-dqlite under maximum load so that
// durability and consistency can be probed at the edges of
// its performance.

import (
	"flag"
	"io"
	"log"
	"os"
	"strings"

	"github.com/canonical/go-dqlite/client"
)

const defaultCluster = "127.0.0.1:9181,127.0.0.1:9182,127.0.0.1:9183"

var (
	cluster  string
	dbName   string
	logLevel string
	count    int
	fail     bool
)

func init() {
	flag.StringVar(&cluster, "cluster", defaultCluster, "list of nodes in the cluster")
	flag.StringVar(&dbName, "database", "demo.db", "name of database to use")
	flag.StringVar(&logLevel, "level", "error", "logging level: {debug|info|warn|error}")
	flag.IntVar(&count, "count", 10000, "how many times to repeat (0 is infinite)")
	flag.BoolVar(&fail, "fail", false, "exit test if data is corrupted)")
}

// NewLogFunc returns a LogFunc.
func NewLogFunc(level LogLevel, prefix string, w io.Writer) LogFunc {
	if w == nil {
		w = os.Stdout
	}
	logger := log.New(w, prefix, log.LstdFlags|log.Lmicroseconds)
	return func(l LogLevel, format string, args ...interface{}) {
		if l >= level {
			// prepend the log level to the message
			args = append([]interface{}{l.String()}, args...)
			format = "[%s] " + format
			logger.Printf(format, args...)
		}
	}
}

func main() {
	flag.Parse()

	var level client.LogLevel
	switch strings.ToLower(logLevel) {
	case "error":
		level = client.LogDebug
	case "warn":
		level = client.LogWarn
	case "info":
		level = client.LogInfo
	case "debug":
		level = client.LogDebug
	default:
		flag.Usage()
		log.Fatalln("not a valid log level:", logLevel)
	}

	logger := NewLogFunc(level, "hammer:", &tsWriter{})
	hammer(count, fail, logger, dbName, strings.Split(cluster, ","))
}
