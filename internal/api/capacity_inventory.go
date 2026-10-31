package api

import (
	"context"
	"sync"
	"time"

	"github.com/orkstack/orkstack/internal/engine"
)

// capacityInventoryTTL is how long one reading of the Orker inventory answers
// InspectRun's capacity question on this replica. It is the live view's own
// poll period (the run page and `orkctl run inspect --watch` ask every 5 s),
// so however many pages and watches are open, a replica asks the Orkers for
// them at most once a period, and a caption is never older than one poll.
const capacityInventoryTTL = 5 * time.Second

// capacityReadTimeout bounds one inventory read. A reading slower than this
// is stale by the time it lands. An Orker that has not answered within it
// keeps every pool from being called full (engine.CapacityWaitsOf), and an
// inventory not read within it leaves every attempt waiting for an Orker.
const capacityReadTimeout = 2 * time.Second

// capacityWarnInterval is how often a replica warns that it could not read
// the inventory for a capacity answer: every open run page asks every few
// seconds, and one warning per interval says as much as one per poll.
const capacityWarnInterval = time.Minute

// sharedInventory is the Orker inventory as InspectRun's capacity question
// reads it: one read in flight at a time, joined by every caller that asks
// while it runs, and a successful reading kept for capacityInventoryTTL. A
// failure is handed to the callers that waited for it and never kept, so the
// next caller asks again — the poolScopeCache rule.
//
// It is a disposable cache (AGENTS.md §13): what it keeps is a reading the
// Orkers gave, never a decision, gone with the process, and every caption it
// feeds is derived again from it on each answer.
type sharedInventory struct {
	fleet engine.FleetReader
	now   func() time.Time

	mu       sync.Mutex
	orkers   []engine.FleetOrker
	readAt   time.Time
	kept     bool
	inflight *inventoryRead
	// joined counts the callers waiting on a read someone else started, for
	// a test to know they wait.
	joined   int
	warnedAt time.Time
}

type inventoryRead struct {
	done   chan struct{}
	orkers []engine.FleetOrker
	err    error
}

func newSharedInventory(fleet engine.FleetReader, now func() time.Time) *sharedInventory {
	return &sharedInventory{fleet: fleet, now: now}
}

// read answers from the reading kept, by joining the read in flight, or by
// starting one. The read runs detached from the caller that started it,
// bounded by capacityReadTimeout, so a viewer who leaves does not fail the
// others; each caller stops waiting when its own context ends.
func (i *sharedInventory) read(ctx context.Context) ([]engine.FleetOrker, error) {
	i.mu.Lock()
	if i.kept && i.now().Sub(i.readAt) < capacityInventoryTTL {
		orkers := i.orkers
		i.mu.Unlock()
		return orkers, nil
	}
	call := i.inflight
	joined := call != nil
	if joined {
		i.joined++
	} else {
		call = &inventoryRead{done: make(chan struct{})}
		i.inflight = call
		go i.fetch(context.WithoutCancel(ctx), call)
	}
	i.mu.Unlock()
	if joined {
		defer func() {
			i.mu.Lock()
			i.joined--
			i.mu.Unlock()
		}()
	}
	select {
	case <-call.done:
		return call.orkers, call.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (i *sharedInventory) fetch(detached context.Context, call *inventoryRead) {
	ctx, cancel := context.WithTimeout(detached, capacityReadTimeout)
	defer cancel()
	call.orkers, call.err = engine.ReadFleet(ctx, i.fleet)

	i.mu.Lock()
	if i.inflight == call {
		i.inflight = nil
	}
	if call.err == nil {
		i.orkers, i.readAt, i.kept = call.orkers, i.now(), true
	}
	i.mu.Unlock()
	close(call.done)
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
