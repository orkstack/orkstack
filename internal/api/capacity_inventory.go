package api

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/orkstack/orkstack/internal/engine"
)

// capacityInventoryTTL is how long one reading of the Orker inventory answers
// InspectRun's capacity question on this replica, counted from when its read
// began. It is the live view's own poll period (the run page and `orkctl run
// inspect --watch` ask every 5 s), so however many pages and watches are
// open, a replica asks the Orkers for them at most once a period, and a
// caption is never older than one poll.
const capacityInventoryTTL = 5 * time.Second

// capacityReadTimeout bounds one inventory read, and so how long any caller
// waits for it. A reading slower than this is stale by the time it lands, and
// an inventory not read within it leaves every attempt waiting for an Orker.
const capacityReadTimeout = 2 * time.Second

// capacityWarnInterval is how often a replica warns that it could not read
// the inventory for a capacity answer: every open run page asks every few
// seconds, and one warning per interval says as much as one per poll.
const capacityWarnInterval = time.Minute

// sharedInventory is the Orker inventory as InspectRun's capacity question
// reads it: one read in flight at a time, joined by every caller that asks
// while it runs, and a whole reading — the presence read succeeded and every
// live Orker answered, within the bound — kept for capacityInventoryTTL. Any
// other outcome is handed to the callers that waited for it and never kept,
// so the next caller asks again: the poolScopeCache rule.
//
// It is a disposable cache (AGENTS.md §13): what it keeps is a reading the
// Orkers gave, never a decision, gone with the process, and every caption it
// feeds is derived again from it on each answer. It keeps one reading because
// the Service has one fleet; should the fleet ever differ per binding
// (engine.Router), the reading must be kept per binding, or one cluster's
// Orkers would answer for another's pools.
type sharedInventory struct {
	fleet engine.FleetReader
	now   func() time.Time
	// bound is how long one read may take, and so how long any caller waits
	// for it (capacityReadTimeout).
	bound time.Duration
	// afterFunc runs f once d has passed and returns what stops it:
	// time.AfterFunc, which a test replaces with a timer it fires itself.
	afterFunc func(d time.Duration, f func()) (stop func() bool)

	mu       sync.Mutex
	orkers   []engine.FleetOrker
	readAt   time.Time // when the read of the reading kept began
	kept     bool
	inflight *inventoryRead
	warnedAt time.Time
}

// inventoryRead is one read and every caller waiting on it. It ends once, at
// whichever comes first of the fleet's answer and the bound (finish).
type inventoryRead struct {
	startedAt time.Time
	done      chan struct{}
	finished  bool // guarded by sharedInventory.mu
	orkers    []engine.FleetOrker
	err       error
}

func newSharedInventory(fleet engine.FleetReader, now func() time.Time) *sharedInventory {
	return &sharedInventory{
		fleet: fleet,
		now:   now,
		bound: capacityReadTimeout,
		afterFunc: func(d time.Duration, f func()) func() bool {
			return time.AfterFunc(d, f).Stop
		},
	}
}

// read answers from the reading kept, by joining the read in flight, or by
// starting one. The read runs detached from the caller that started it, so a
// viewer who leaves does not fail the others, and it ends at its bound whether
// or not the fleet has answered: a caller waits at most that long, or until
// its own context ends.
func (i *sharedInventory) read(ctx context.Context) ([]engine.FleetOrker, error) {
	i.mu.Lock()
	now := i.now()
	if i.kept && now.Sub(i.readAt) < capacityInventoryTTL {
		orkers := i.orkers
		i.mu.Unlock()
		return orkers, nil
	}
	call := i.inflight
	if call == nil {
		call = &inventoryRead{startedAt: now, done: make(chan struct{})}
		i.inflight = call
		readCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		stop := i.afterFunc(i.bound, func() {
			cancel()
			i.finish(call, nil, fmt.Errorf("the Orker inventory was not read within %s", i.bound), false)
		})
		go i.fetch(readCtx, cancel, stop, call)
	}
	i.mu.Unlock()
	select {
	case <-call.done:
		return call.orkers, call.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// fetch reads the inventory for call. The bound's timer cancels readCtx and
// ends call without it; a fleet that ignores the cancellation keeps this
// goroutine until it returns, and what it returns then is dropped.
func (i *sharedInventory) fetch(readCtx context.Context, cancel context.CancelFunc, stop func() bool, call *inventoryRead) {
	defer cancel()
	orkers, err := engine.ReadFleet(readCtx, i.fleet)
	stop()
	i.finish(call, orkers, err, err == nil && everyOrkerAnswered(orkers))
}

// finish ends call the first time it is called and does nothing after: it
// clears the read in flight, so the next caller starts another, keeps the
// reading when whole, and answers every caller waiting on call.
func (i *sharedInventory) finish(call *inventoryRead, orkers []engine.FleetOrker, err error, whole bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if call.finished {
		return
	}
	call.finished = true
	call.orkers, call.err = orkers, err
	if i.inflight == call {
		i.inflight = nil
	}
	if whole {
		i.orkers, i.readAt, i.kept = orkers, call.startedAt, true
	}
	close(call.done)
}

// everyOrkerAnswered reports whether every live Orker in a reading answered
// on its own channel. A reading one did not is handed to its callers, who say
// nothing of capacity from it (engine.CapacityWaitsOf), and never kept.
func everyOrkerAnswered(orkers []engine.FleetOrker) bool {
	for _, orker := range orkers {
		if orker.Err != nil {
			return false
		}
	}
	return true
}

// warnDue reports whether a failed read should be warned of now: once per
// capacityWarnInterval per replica.
func (i *sharedInventory) warnDue() bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	now := i.now()
	if !i.warnedAt.IsZero() && now.Sub(i.warnedAt) < capacityWarnInterval {
		return false
	}
	i.warnedAt = now
	return true
}
