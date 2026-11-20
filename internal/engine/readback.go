package engine

import (
	"fmt"
	"time"
)

// A resume's read-back (E85 §4.3, step 3): the kept tasks' results are read
// back from their commits by AdoptRecorded, a local activity on the engine's
// workflow worker, in calls the workflow packs and issues in task order.
// Every number here is pinned per run in the resume's block (AGENTS.md §7.2)
// and comes from readbackTunables, never from a constant at decision time;
// the defaults below are what ResumeRun pins.

// readbackTunables is the read-back's pinned numbers (§4.1, the block).
type readbackTunables struct {
	// Deadline bounds the whole phase, from RUN_RESUMED.
	Deadline time.Duration
	// Budget bounds one call's encoded result, framing included.
	Budget int64
	// MaxEntries bounds the entries of one call.
	MaxEntries int
	// Concurrency bounds the reads one call has in flight.
	Concurrency int
	// GetTimeout bounds one read's tries, at most GetTries of them.
	GetTimeout time.Duration
	GetTries   int
	// CallTimeout bounds one attempt of a call (StartToClose).
	CallTimeout time.Duration
	// RetryInitial and RetryCoefficient are the call's retry backoff; its
	// ceiling is derived from the workflow task timeout (readbackCallTimeouts).
	RetryInitial     time.Duration
	RetryCoefficient float64
	// CallsPerTask bounds the calls issued per workflow task.
	CallsPerTask int
}

func defaultReadbackTunables() readbackTunables {
	return readbackTunables{
		Deadline:         10 * time.Minute,
		Budget:           128 << 10,
		MaxEntries:       128,
		Concurrency:      16,
		GetTimeout:       10 * time.Second,
		GetTries:         3,
		CallTimeout:      90 * time.Second,
		RetryInitial:     time.Second,
		RetryCoefficient: 2,
		CallsPerTask:     4,
	}
}

// readbackRetryCeiling is the fixed upper bound of a call's retry interval
// (§4.3, GT15 M4): retries stay inside one workflow task, in process, so a
// failed attempt costs no history event.
const readbackRetryCeiling = 5 * time.Second

// readbackFraming bounds the encoded bytes one call's result adds around
// its entries: the result's own braces and field name, and each entry's
// separator. An entry's size counts the entry whole, its task id and item
// included, so the framing does not grow with a task id's length. It is at
// most 1 KiB for 128 entries (§4.3).
func readbackFraming(entries int) int64 {
	const header, perEntry = 64, 2
	return header + perEntry*int64(entries)
}

// packReadback packs the entries, by their encoded sizes in task order, into
// calls of consecutive entries, as [start, end) ranges. A call closes before
// an entry that would take it past the budget, framing counted, or that
// would be its MaxEntries+1th; a call of one entry is always permitted, and
// an entry that no call could hold is refused, because ResumeRun's
// preflight bounds every entry to fit alone (§3.5).
func packReadback(sizes []int64, tun readbackTunables) ([][2]int, error) {
	var calls [][2]int
	start, sum := 0, int64(0)
	for i, size := range sizes {
		if size < 0 {
			return nil, fmt.Errorf("read-back entry %d has a negative size %d", i, size)
		}
		if size+readbackFraming(1) > tun.Budget {
			return nil, fmt.Errorf("read-back entry %d is %d bytes; a call holds at most %d with its framing",
				i, size, tun.Budget-readbackFraming(1))
		}
		n := i - start
		if n > 0 && (n == tun.MaxEntries || sum+size+readbackFraming(n+1) > tun.Budget) {
			calls = append(calls, [2]int{start, i})
			start, sum = i, 0
		}
		sum += size
	}
	if start < len(sizes) {
		calls = append(calls, [2]int{start, len(sizes)})
	}
	return calls, nil
}

// readbackTimeouts are one call's local-activity bounds.
type readbackTimeouts struct {
	startToClose    time.Duration
	scheduleToClose time.Duration
	maxInterval     time.Duration
}

// readbackCallTimeouts derives one call's bounds at now (§4.3): its
// ScheduleToClose is the time left to the phase's recorded absolute
// deadline, so a call re-executed after a lost workflow task never extends
// the phase, and its retry ceiling is min(5 s, half the workflow task
// timeout), always below that timeout. false means no call may be issued:
// the deadline has passed, or the task timeout is unknown.
func readbackCallTimeouts(tun readbackTunables, workflowTaskTimeout time.Duration, now, deadline time.Time) (readbackTimeouts, bool) {
	left := deadline.Sub(now)
	if left <= 0 || workflowTaskTimeout <= 0 {
		return readbackTimeouts{}, false
	}
	ceiling := min(readbackRetryCeiling, workflowTaskTimeout/2) // below the task timeout for any positive one
	return readbackTimeouts{startToClose: tun.CallTimeout, scheduleToClose: left, maxInterval: ceiling}, true
}
