package orker

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/worker"
)

// recordingObserver is a CapacityObserver that keeps what it was told.
type recordingObserver struct {
	mu       sync.Mutex
	orkerID  string
	read     func() CapacityReading
	watches  int
	waits    []string
	waited   []time.Duration
	refusals int
}

func (r *recordingObserver) Watch(orkerID string, read func() CapacityReading) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.orkerID, r.read = orkerID, read
	r.watches++
}

func (r *recordingObserver) PollWaited(waited time.Duration, bound string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.waits = append(r.waits, bound)
	r.waited = append(r.waited, waited)
}

func (r *recordingObserver) AdmissionRefused() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.refusals++
}

func (r *recordingObserver) heard() ([]string, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.waits...), r.refusals
}

// waitThenGrant parks one poll in reserve, frees what holds it, and returns
// once it is granted.
func waitThenGrant(t *testing.T, c *Capacity, free func()) *worker.SlotPermit {
	t.Helper()
	granted := make(chan *worker.SlotPermit, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		permit, _ := c.reserve(ctx, nil)
		granted <- permit
	}()
	awaitWaiting(t, c, 1)
	free()
	permit := <-granted
	if permit == nil {
		t.Fatal("the waiting poll was never granted")
	}
	return permit
}

// A poll granted at once is not a wait; one that waited is reported once it
// is granted, with what held it: the capacity, or every slot taken.
func TestCapacity_TellsItsObserverWhatAPollWaitedFor(t *testing.T) {
	obs := &recordingObserver{}
	c := newTestCapacity()
	c.observer = obs
	first, second := mustReserve(t, c), mustReserve(t, c)
	if waits, _ := obs.heard(); len(waits) != 0 {
		t.Fatalf("polls granted at once reported waits %v, want none", waits)
	}
	third := waitThenGrant(t, c, func() { c.release(first) })
	if waits, _ := obs.heard(); len(waits) != 1 || waits[0] != AdmissionBoundCapacity {
		t.Fatalf("waits = %v, want one held by %s", waits, AdmissionBoundCapacity)
	}
	// It waited from before the release until the grant, so it waited for
	// some time; a zero would be a duration measured from the wrong instant.
	if obs.waited[0] <= 0 {
		t.Fatalf("the wait was reported as %s, want the time it waited", obs.waited[0])
	}
	c.release(second)
	c.release(third)

	slots := newCapacity(amount{}, small, 1) // no size limit: one slot binds
	slots.observer = obs
	only := mustReserve(t, slots)
	next := waitThenGrant(t, slots, func() { slots.release(only) })
	if waits, _ := obs.heard(); len(waits) != 2 || waits[1] != AdmissionBoundSlots {
		t.Fatalf("waits = %v, want a second one held by %s", waits, AdmissionBoundSlots)
	}
	slots.release(next)
}

// A refusal for a request larger than its poll is reported, and an admission
// that fits is not.
func TestCapacity_TellsItsObserverOfEveryRefusal(t *testing.T) {
	obs := &recordingObserver{}
	c := newTestCapacity()
	c.observer = obs
	p, q := mustReserve(t, c), mustReserve(t, c)
	c.markUsed(p)
	if _, err := c.admit(true, amount{milli: 3000, bytes: 1 << 30}); err == nil {
		t.Fatal("a request larger than its poll with no room was admitted")
	}
	c.markUsed(q)
	if _, err := c.admit(true, small); err != nil {
		t.Fatalf("a request within its poll was refused: %v", err)
	}
	if _, refusals := obs.heard(); refusals != 1 {
		t.Fatalf("refusals reported = %d, want 1", refusals)
	}
}

// An Orker declaring a capacity hands its observer a read of its ledger once,
// under its own id; one declaring none hands it nothing.
func TestActivities_HandTheCapacityObserverAReadOfTheLedger(t *testing.T) {
	policy := DefaultSecurityPolicy() // a cap of 8 cpu, 16Gi
	obs := &recordingObserver{}
	activities := NewActivitiesWithOptions(nil, Options{
		Policy: &policy, Runtime: &layoutProbeRuntime{}, OrkerID: "o1",
		CapacityCPU: "16", CapacityMemory: "64Gi", TaskQueue: "q", CapacityObserver: obs,
	})
	if obs.watches != 1 || obs.orkerID != "o1" || obs.read == nil {
		t.Fatalf("watches = %d under %q, want one under o1", obs.watches, obs.orkerID)
	}
	if got := obs.read(); got != (CapacityReading{CapacityCPU: 16, CapacityMemory: 64 << 30}) {
		t.Fatalf("idle reading = %+v, want 16 cpu and 64Gi with nothing reserved", got)
	}
	permit := mustReserve(t, activities.capacity)
	if got := obs.read(); got.ReservedCPU != 8 || got.ReservedMemory != 16<<30 {
		t.Fatalf("reading with one poll out = %+v, want the cap (8 cpu, 16Gi) reserved", got)
	}
	activities.capacity.release(permit)

	plain := &recordingObserver{}
	NewActivitiesWithOptions(nil, Options{Policy: &policy, Runtime: &layoutProbeRuntime{}, OrkerID: "o2", CapacityObserver: plain})
	if plain.watches != 0 {
		t.Fatalf("an Orker declaring no capacity was watched %d times, want none", plain.watches)
	}
}
