package metrics_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/canonical/go-dqlite/v3/logging"
	"github.com/canonical/go-dqlite/v3/metrics"
	"github.com/stretchr/testify/assert"
)

type observation struct {
	command  metrics.Command
	result   metrics.CommandResult
	duration time.Duration
	query    string
}

type recorder struct {
	commands []observation
	cache    []metrics.CacheResult
}

func (r *recorder) ObserveCommand(command metrics.Command, result metrics.CommandResult, duration time.Duration, query string) {
	r.commands = append(r.commands, observation{command: command, result: result, duration: duration, query: query})
}

func (r *recorder) ObserveCache(result metrics.CacheResult) {
	r.cache = append(r.cache, result)
}

func TestObserve(t *testing.T) {
	recorder := &recorder{}
	duration := 10 * time.Millisecond

	metrics.ObserveCommand(recorder, metrics.CommandExec, duration, "INSERT", nil)
	metrics.ObserveCommand(recorder, metrics.CommandQuery, duration, "SELECT", errors.New("boom"))
	metrics.ObserveCache(recorder, metrics.CacheHit)

	assert.Equal(t, []observation{
		{command: metrics.CommandExec, result: metrics.CommandSuccess, duration: duration, query: "INSERT"},
		{command: metrics.CommandQuery, result: metrics.CommandError, duration: duration, query: "SELECT"},
	}, recorder.commands)
	assert.Equal(t, []metrics.CacheResult{metrics.CacheHit}, recorder.cache)
}

func TestNilRecorder(t *testing.T) {
	assert.NotPanics(t, func() {
		metrics.ObserveCommand(nil, metrics.CommandPrepare, 0, "SELECT", nil)
		metrics.ObserveCache(nil, metrics.CacheMiss)
	})
}

func TestCombine(t *testing.T) {
	first := &recorder{}
	second := &recorder{}
	recorder := metrics.Combine(nil, first, second)

	metrics.ObserveCache(recorder, metrics.CacheMiss)
	metrics.ObserveCommand(recorder, metrics.CommandPrepare, time.Second, "SELECT 1", nil)

	assert.Equal(t, first.cache, second.cache)
	assert.Equal(t, first.commands, second.commands)
	assert.Nil(t, metrics.Combine(nil, nil))
	assert.Same(t, first, metrics.Combine(first))
}

func TestLogRecorder(t *testing.T) {
	var level logging.Level
	var message string
	recorder := metrics.NewLogRecorder(func(gotLevel logging.Level, format string, args ...interface{}) {
		level = gotLevel
		message = fmt.Sprintf(format, args...)
	}, logging.Info)

	metrics.ObserveCommand(recorder, metrics.CommandQuery, 1500*time.Millisecond, "SELECT 1", nil)
	metrics.ObserveCache(recorder, metrics.CacheHit)

	assert.Equal(t, logging.Info, level)
	assert.Equal(t, `1.500s request query: "SELECT 1"`, message)
}
