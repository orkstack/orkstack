package engine

import (
	"time"
)

// ProjectionRetention is how long an engine that answers reads from Temporal
// and the archive keeps a finished run in its RunStore and EventBus. It is
// far longer than the projection activity's whole retry window
// (eventActivityTimeout × its 3 attempts, plus backoff), so a retried
// terminal publish still finds the run's dedupe keys and merged snapshot.
const ProjectionRetention = 10 * time.Minute

// terminalRetention is how RunStore and EventBus forget finished runs.
//
// Both are write-side helpers (AGENTS.md §7): with a RunReader wired, no read
// is answered from them, so a run they still hold after it finished only
// serves the projection activity's own retries — the merged snapshot and the
// dedupe of a retried terminal publish. Past that window it is accumulation.
//
// A run is MARKED when its terminal snapshot is applied or its terminal event
// published, and forgotten by the owner on a later write once it has been
// marked for longer than ttl. There is no goroutine: an idle process holds
// what it held, and the first write after idleness pays the sweep. A ttl of
// zero keeps everything, which is the in-process read model a deployment with
// no reader (and every unit test) depends on.
//
// The caller holds its own lock around every method.
type terminalRetention struct {
	ttl    time.Duration
	now    func() time.Time
	marked map[string]time.Time
	// queue is the marks in the order they were made. The clock is the same
	// for every mark, so the queue is ordered by time and a sweep stops at the
	// first mark that is not yet due. A run unmarked or re-queued leaves a
	// stale entry behind, which a sweep recognises by its time and drops.
	queue []terminalMark
}

type terminalMark struct {
	runID string
	at    time.Time
}

func newTerminalRetention() terminalRetention {
	return terminalRetention{now: time.Now, marked: map[string]time.Time{}}
}

// enabled reports whether finished runs are forgotten at all.
func (r *terminalRetention) enabled() bool { return r.ttl > 0 }

// mark records that runID finished now. A run already marked keeps its first
// mark, so a retried terminal write never extends its own window.
func (r *terminalRetention) mark(runID string) {
	if !r.enabled() {
		return
	}
	if _, ok := r.marked[runID]; ok {
		return
	}
	at := r.now()
	r.marked[runID] = at
	r.queue = append(r.queue, terminalMark{runID: runID, at: at})
}

// unmark forgets a mark: the run is not finished after all, or it is gone.
func (r *terminalRetention) unmark(runID string) {
	delete(r.marked, runID)
}

// due pops every run marked longer than ttl ago and returns it, except a run
// held reports true for: that one is re-marked now and examined again one
// ttl later.
func (r *terminalRetention) due(held func(runID string) bool) []string {
	if !r.enabled() || len(r.queue) == 0 {
		return nil
	}
	now := r.now()
	var (
		out      []string
		requeued []terminalMark
		popped   int
	)
	for _, mark := range r.queue {
		if now.Sub(mark.at) < r.ttl {
			break
		}
		popped++
		at, ok := r.marked[mark.runID]
		if !ok || !at.Equal(mark.at) {
			continue // stale: unmarked, or re-queued since
		}
		if held != nil && held(mark.runID) {
			r.marked[mark.runID] = now
			requeued = append(requeued, terminalMark{runID: mark.runID, at: now})
			continue
		}
		delete(r.marked, mark.runID)
		out = append(out, mark.runID)
	}
	if popped > 0 {
		r.queue = append(r.queue[:0:0], r.queue[popped:]...)
		r.queue = append(r.queue, requeued...)
	}
	return out
}
