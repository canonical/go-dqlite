package client

import (
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

type writeCheck bool

func (w *writeCheck) Write(in []byte) (int, error) {
	*w = true
	return len(in), nil
}

func TestNewLogFunc(t *testing.T) {
	// first with nil to exercise the stdout assignment
	logger := NewLogFunc(LogError, "", nil)

	// now verify levels are respected
	w := new(writeCheck)
	logger = NewLogFunc(LogError, "", w)
	logger(LogDebug, "hello")
	if *w {
		t.Fatal("log level ignored")
	}
	logger(LogError, "hello")
	if !*w {
		t.Fatal("log level did not print")
	}
}

func TestLoggingWriter(t *testing.T) {
	// now verify levels are respected
	w := new(writeCheck)
	log.SetOutput(w)
	logger := NewLogFunc(LogError, "", NewLoggingWriter())
	logger(LogDebug, "hello")
	if *w {
		t.Fatal("log level ignored")
	}
	logger(LogError, "hello")
	if !*w {
		t.Fatal("log level did not print")
	}
}

func TestNewLogLevel(t *testing.T) {
	l, err := NewLogLevel("debug")
	assert.NoError(t, err)
	assert.Equal(t, l, LogDebug)

	l, err = NewLogLevel("info")
	assert.NoError(t, err)
	assert.Equal(t, l, LogInfo)

	l, err = NewLogLevel("warn")
	assert.NoError(t, err)
	assert.Equal(t, l, LogWarn)

	l, err = NewLogLevel("error")
	assert.NoError(t, err)
	assert.Equal(t, l, LogError)

	l, err = NewLogLevel("invalid")
	assert.Error(t, err)
}
