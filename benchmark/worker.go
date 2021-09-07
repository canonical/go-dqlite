package benchmark

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"
)

type work int
type workerType int

func (w work) String() string {
	switch w {
	case exec:
		return "exec"
	case query:
		return "query"
	case none:
		return "none"
	default:
		return "unknown"
	}
}

const (
	// The type of query to perform
	none  work = iota
	exec  work = iota // a `write`
	query work = iota // a `read`

	kvWriter       workerType = iota
	kvReader       workerType = iota
	kvReaderWriter workerType = iota

	kvReadSql  = "SELECT value FROM model WHERE key = ?"
	kvWriteSql = "INSERT OR REPLACE INTO model(key, value) VALUES(?, ?)"
)

// A worker performs the queries to the database and keeps around some state
// in order to do that. `lastWork` and `lastArgs` refer to the previously
// executed operation and can be used to determine the next work the worker
// should perform. `kvKeys` tells the worker which keys it has inserted in the
// database.
type worker struct {
	workerType   workerType
	lastWork     work
	lastArgs     []interface{}
	tracker      *tracker
	kvKeySizeB   int
	kvValueSizeB int
	kvKeys       []string
}

// Thanks to https://stackoverflow.com/a/22892986
var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func (w *worker) randNewKey() string {
	return randSeq(w.kvKeySizeB)
}

func (w *worker) randExistingKey() (string, error) {
	n := len(w.kvKeys)
	if n == 0 {
		return "", errors.New("no keys")
	}
	return w.kvKeys[rand.Intn(n)], nil
}

// A mix of random bytes and easily compressable bytes.
func (w *worker) randValue() string {
	return strings.Repeat(randSeq(1), w.kvValueSizeB/2) + randSeq(w.kvValueSizeB/2)
}

// Returns the type of work to execute and a sql statement with arguments
func (w *worker) getWork() (work, string, []interface{}) {
	switch w.workerType {
	case kvWriter:
		k, v := w.randNewKey(), w.randValue()
		return exec, kvWriteSql, []interface{}{k, v}
	case kvReaderWriter:
		read := rand.Intn(2) == 0
		if read && len(w.kvKeys) != 0 {
			k, _ := w.randExistingKey()
			return query, kvReadSql, []interface{}{k}
		}
		k, v := w.randNewKey(), w.randValue()
		return exec, kvWriteSql, []interface{}{k, v}
	default:
		return none, "", []interface{}{}
	}
}

// Retrieve a query and execute it against the database
func (w *worker) doWork(ctx context.Context, db *sql.DB) {
	var err error
	var str string

	work, q, args := w.getWork()
	w.lastWork = work
	w.lastArgs = args

	switch work {
	case exec:
		w.kvKeys = append(w.kvKeys, fmt.Sprintf("%v", (args[0])))
		defer w.tracker.measure(time.Now(), work, &err)
		_, err = db.ExecContext(ctx, q, args...)
		if err != nil {
			w.kvKeys = w.kvKeys[:len(w.kvKeys)-1]
		}
	case query:
		defer w.tracker.measure(time.Now(), work, &err)
		err = db.QueryRowContext(ctx, q, args...).Scan(&str)
	default:
		return
	}
}

func (w *worker) run(ctx context.Context, db *sql.DB) {
	for {
		if ctx.Err() != nil {
			return
		}

		w.doWork(ctx, db)
	}
}

func (w *worker) report() map[work]report {
	return w.tracker.report()
}

func newWorker(workerType workerType, o *options) *worker {
	return &worker{
		workerType:   workerType,
		kvKeySizeB:   o.kvKeySizeB,
		kvValueSizeB: o.kvValueSizeB,
		tracker:      newTracker(),
	}
}
