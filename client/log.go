package client

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/canonical/go-dqlite/internal/logging"
)

// LogFunc is a function that can be used for logging.
type LogFunc = logging.Func

// LogLevel defines the logging level.
type LogLevel = logging.Level

// Available logging levels.
const (
	LogDebug = logging.Debug
	LogInfo  = logging.Info
	LogWarn  = logging.Warn
	LogError = logging.Error
)

// DefaultLogFunc doesn't emit any message.
func DefaultLogFunc(l LogLevel, format string, a ...interface{}) {
}

type logWriter struct{}

func (l *logWriter) Write(in []byte) (int, error) {
	log.Println(string(in))
	return len(in), nil
}

// NewLoggingWriter returns an io.Writer using the default Go logger
func NewLoggingWriter() io.Writer {
	return &logWriter{}
}

// NewLogFunc returns a LogFunc.
//
// If no writer is specified it will use stdout
func NewLogFunc(level LogLevel, prefix string, w io.Writer) LogFunc {
	if w == nil {
		w = os.Stdout
	}
	return func(l LogLevel, format string, args ...interface{}) {
		if l >= level {
			// prepend the log level to the message
			args = append([]interface{}{l.String()}, args...)
			format = prefix + "[%s] " + format
			fmt.Fprintf(w, format, args...)
		}
	}
}
