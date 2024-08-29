package utils_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/canonical/go-dqlite/internal/utils"
)

func TestAfterFuncCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := make(chan struct{})
	stop := utils.AfterFunc(ctx, func() {
		close(c)
	})
	// Function should run immediately so stop() should not affect result.
	assert.Equal(t, false, stop())
	<-c
}

func TestAfterFuncUncancelableContext(t *testing.T) {
	utils.AfterFunc(context.Background(), func() {
		panic("should never run")
	})
	time.Sleep(time.Millisecond * 200)
}

func TestAfterFuncNormalOperation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var flag int32 = 0
	done := make(chan struct{})
	utils.AfterFunc(ctx, func() {
		atomic.StoreInt32(&flag, 1)
		close(done)
	})
	time.Sleep(time.Millisecond * 200)
	assert.Equal(t, int32(0), atomic.LoadInt32(&flag))
	cancel()
	<-done
	assert.Equal(t, int32(1), atomic.LoadInt32(&flag))
}

func TestAfterFuncStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := utils.AfterFunc(ctx, func() {
		panic("should never run")
	})
	assert.Equal(t, true, stop())
}

func TestAfterFuncStopAlreadyRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	stop := utils.AfterFunc(ctx, func() {
		close(done)
	})
	cancel()
	<-done
	assert.Equal(t, false, stop())
}
