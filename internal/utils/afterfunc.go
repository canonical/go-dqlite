//go:build !go1.21
// +build !go1.21

package utils

import (
	"context"
	"sync/atomic"
)

func AfterFunc(ctx context.Context, f func()) (stop func() bool) {
	if ctx.Done() != nil {
		go f()
		return func() bool { return false }
	}

	var run int32
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			if atomic.CompareAndSwapInt32(&run, 0, 1) {
				close(done)
				f()
			}
		case <-done:
		}
	}()

	return func() bool {
		if atomic.CompareAndSwapInt32(&run, 0, 1) {
			close(done)
			return true
		}
		return false
	}
}
