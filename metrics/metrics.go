// Package metrics defines the metrics emitted by the dqlite driver.
//
// The package is deliberately independent of any metrics backend. Callers can
// adapt Recorder to Prometheus, OpenTelemetry, or another collection system.
package metrics

import (
	"time"

	"github.com/canonical/go-dqlite/v3/logging"
)

// Command identifies a dqlite statement command.
type Command string

const (
	CommandPrepare Command = "prepare"
	CommandExec    Command = "exec"
	CommandQuery   Command = "query"
)

// CommandResult identifies whether a command succeeded.
type CommandResult string

const (
	CommandSuccess CommandResult = "success"
	CommandError   CommandResult = "error"
)

// CacheResult identifies the result of a prepared-statement cache lookup.
type CacheResult string

const (
	CacheHit      CacheResult = "hit"
	CacheMiss     CacheResult = "miss"
	CacheDisabled CacheResult = "disabled"
)

// Recorder receives driver metrics. Implementations must be safe for
// concurrent use because one recorder can be shared by many connections. The
// query argument is intended for diagnostics and should not be used as an
// unbounded metrics label.
type Recorder interface {
	ObserveCommand(Command, CommandResult, time.Duration, string)
	ObserveCache(CacheResult)
}

// ObserveCommand records the duration and result of a command. A nil recorder
// is treated as disabled metrics collection.
func ObserveCommand(recorder Recorder, command Command, duration time.Duration, query string, err error) {
	if recorder == nil {
		return
	}
	result := CommandSuccess
	if err != nil {
		result = CommandError
	}
	recorder.ObserveCommand(command, result, duration, query)
}

// Combine returns a recorder that forwards observations to each non-nil
// recorder. It returns nil when no recorders are supplied.
func Combine(recorders ...Recorder) Recorder {
	nonNil := make([]Recorder, 0, len(recorders))
	for _, recorder := range recorders {
		if recorder != nil {
			nonNil = append(nonNil, recorder)
		}
	}
	switch len(nonNil) {
	case 0:
		return nil
	case 1:
		return nonNil[0]
	default:
		return combinedRecorder(nonNil)
	}
}

type combinedRecorder []Recorder

func (r combinedRecorder) ObserveCommand(command Command, result CommandResult, duration time.Duration, query string) {
	for _, recorder := range r {
		recorder.ObserveCommand(command, result, duration, query)
	}
}

func (r combinedRecorder) ObserveCache(result CacheResult) {
	for _, recorder := range r {
		recorder.ObserveCache(result)
	}
}

// NewLogRecorder returns a recorder that logs command durations at level.
// Cache observations are ignored because WithTracing historically only
// logged statement commands.
func NewLogRecorder(log logging.Func, level logging.Level) Recorder {
	return logRecorder{log: log, level: level}
}

type logRecorder struct {
	log   logging.Func
	level logging.Level
}

func (r logRecorder) ObserveCommand(command Command, _ CommandResult, duration time.Duration, query string) {
	r.log(r.level, "%.3fs request %s: %q", duration.Seconds(), command, query)
}

func (logRecorder) ObserveCache(CacheResult) {}

// ObserveCache records a prepared-statement cache lookup. A nil recorder is
// treated as disabled metrics collection.
func ObserveCache(recorder Recorder, result CacheResult) {
	if recorder != nil {
		recorder.ObserveCache(result)
	}
}
