package client

import (
	"github.com/canonical/go-dqlite/logging"
)

// LogFunc is a function that can be used for logging.
type LogFunc = logging.Func

// LogLevel defines the logging level.
type LogLevel = logging.Level

// Available logging levels.
const (
	LogNone  = logging.None
	LogDebug = logging.Debug
	LogInfo  = logging.Info
	LogWarn  = logging.Warn
	LogError = logging.Error
)

// DefaultLogFunc doesn't emit any message.
func DefaultLogFunc(l LogLevel, format string, a ...interface{}) {}
