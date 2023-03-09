package benchmark

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

func durToMs(d time.Duration) string {
	ms := int64(d / time.Millisecond)
	rest := int64(d % time.Millisecond)
	return fmt.Sprintf("%d.%06d", ms, rest)
}

type measurement struct {
	start    time.Time
	duration time.Duration
}

func (m measurement) String() string {
	return fmt.Sprintf("%v %v", m.start.UnixNano(), durToMs(m.duration))
}

type measurementErr struct {
	start time.Time
	err   error
}

func (m measurementErr) String() string {
	return fmt.Sprintf("%v %v", m.start.UnixNano(), m.err)
}

type tracker struct {
	lock         sync.RWMutex
	measurements map[work][]measurement
	errors       map[work][]measurementErr
}

type report struct {
	n             int
	nErr          int
	totalDuration time.Duration
	avgDuration   time.Duration
	maxDuration   time.Duration
	minDuration   time.Duration
	measurements  []measurement
	errors        []measurementErr
}

func (r report) String() string {
	var msb strings.Builder
	for _, m := range r.measurements {
		fmt.Fprintf(&msb, "%s\n", m)
	}

	var esb strings.Builder
	for _, e := range r.errors {
		fmt.Fprintf(&esb, "%s\n", e)
	}

	return fmt.Sprintf("n %d\n"+
		"n_err %d\n"+
		"avg [ms] %s\n"+
		"max [ms] %s\n"+
		"min [ms] %s\n"+
		"measurements [timestamp in ns] [ms]\n%s\n"+
		"errors\n%s\n",
		r.n, r.nErr, durToMs(r.avgDuration),
		durToMs(r.maxDuration), durToMs(r.minDuration),
		msb.String(), esb.String())
}

func (t *tracker) measure(start time.Time, work work, err *error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	duration := time.Since(start)
	if *err == nil {
		m := measurement{start, duration}
		t.measurements[work] = append(t.measurements[work], m)
	} else {
		e := measurementErr{start, *err}
		t.errors[work] = append(t.errors[work], e)
	}
}

func (t *tracker) report() map[work]report {
	t.lock.RLock()
	defer t.lock.RUnlock()
	reports := make(map[work]report)
	for w := range t.measurements {
		report := report{
			n:             len(t.measurements[w]),
			nErr:          len(t.errors[w]),
			totalDuration: 0,
			avgDuration:   0,
			maxDuration:   0,
			minDuration:   time.Duration(math.MaxInt64),
			measurements:  t.measurements[w],
			errors:        t.errors[w],
		}

		for _, m := range t.measurements[w] {
			report.totalDuration += m.duration
			if m.duration < report.minDuration {
				report.minDuration = m.duration
			}
			if m.duration > report.maxDuration {
				report.maxDuration = m.duration
			}
		}

		if report.n > 0 {
			report.avgDuration = report.totalDuration / time.Duration(report.n)
		}
		reports[w] = report
	}

	return reports
}

func newTracker() *tracker {
	return &tracker{
		lock:         sync.RWMutex{},
		measurements: make(map[work][]measurement),
		errors:       make(map[work][]measurementErr),
	}
}
