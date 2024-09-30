package protocol

import (
	"time"

	"github.com/Rican7/retry/backoff"
	"github.com/Rican7/retry/strategy"
)

// Config holds various configuration parameters for a dqlite client.
type Config struct {
	Dial                  DialFunc      // Network dialer.
	DialTimeout           time.Duration // Timeout for establishing a network connection .
	AttemptTimeout        time.Duration // Timeout for each individual attempt to probe a server's leadership.
	RetryLimit            uint          // Maximum number of retries, or 0 for unlimited.
	BackoffFactor         time.Duration // Exponential backoff factor for retries.
	BackoffCap            time.Duration // Maximum connection retry backoff value,
	ConcurrentLeaderConns int64         // Maximum number of concurrent connections to other cluster members while probing for leadership.
	PermitShared          bool          // Whether it's okay to return a reused connection.
}

// RetryStrategies returns a configuration for the retry package based on a Config.
func (config Config) RetryStrategies() (strategies []strategy.Strategy) {
	limit, factor, cap := config.RetryLimit, config.BackoffFactor, config.BackoffCap
	// Fix for change in behavior: https://github.com/Rican7/retry/pull/12
	if limit++; limit > 1 {
		strategies = append(strategies, strategy.Limit(limit))
	}
	backoffFunc := backoff.BinaryExponential(factor)
	strategies = append(strategies,
		func(attempt uint) bool {
			if attempt > 0 {
				duration := backoffFunc(attempt)
				// Duration might be negative in case of integer overflow.
				if !(0 < duration && duration <= cap) {
					duration = cap
				}
				time.Sleep(duration)
			}
			return true
		},
	)
	return
}
