package orker

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.temporal.io/sdk/worker"
)

// The ledger as a state machine, driven in every order the SDK and the
// dispatcher can produce: reserve (before a poll), markUsed (a task was
// delivered), admit (the interceptor has the task), the container's creation
// and removal, Done (the activity returned) and release (the SDK frees the
// permit). The reservation is checked at the steps that matter, and every
// order must leave the ledger at rest.

var (
	testCeiling = amount{milli: 2000, bytes: 4 << 30}
	testLimit   = amount{milli: 4000, bytes: 8 << 30}
	small       = amount{milli: 500, bytes: 1 << 30}
)

func newTestCapacity() *Capacity { return newCapacity(testLimit, testCeiling, 16) }

func (c *Capacity) reservedNow() amount {
	_, reserved := c.snapshot()
	return reserved
}

func mustReserve(t *testing.T, c *Capacity) *worker.SlotPermit {
	t.Helper()
	permit := c.tryReserve()
	if permit == nil {
		t.Fatalf("no poll granted with %s reserved of %s", c.reservedNow(), testLimit)
	}
	return permit
}

func wantReserved(t *testing.T, c *Capacity, step string, want amount) {
	t.Helper()
	if got := c.reservedNow(); got != want {
		t.Fatalf("after %s: reserved %s, want %s", step, got, want)
	}
}

func TestCapacity_APollIsGrantedOnlyWhileTheCapFits(t *testing.T) {
	c := newTestCapacity()
	first, second := mustReserve(t, c), mustReserve(t, c)
	wantReserved(t, c, "two polls", testCeiling.add(testCeiling))
	if c.tryReserve() != nil {
		t.Fatal("a third poll was granted with no cap's worth free")
	}
	c.release(first) // an empty poll
	wantReserved(t, c, "an empty poll released", testCeiling)
	third := mustReserve(t, c)
	c.release(second)
	c.release(third)
	wantReserved(t, c, "every empty poll released", amount{})
}

func TestCapacity_CountsSlotsAsWellAsSize(t *testing.T) {
	c := newCapacity(amount{}, small, 2) // no size limit: only max_concurrent binds
	mustReserve(t, c)
	mustReserve(t, c)
	if c.tryReserve() != nil {
		t.Fatal("a poll was granted past max_concurrent")
	}
}

func TestCapacity_EveryOrderLeavesNothingReserved(t *testing.T) {
	removeFailed := errors.New("removal in progress")
	for name, steps := range map[string]func(t *testing.T, c *Capacity){
		"a task that ran a container": func(t *testing.T, c *Capacity) {
			p := mustReserve(t, c)
			c.markUsed(p)
			a, err := c.admit(true, small)
			if err != nil {
				t.Fatal(err)
			}
			wantReserved(t, c, "admit", small)
			a.ContainerCreated("c1")
			a.ContainerRemoved(nil)
			wantReserved(t, c, "removal", amount{})
			a.Done()
			c.release(p)
		},
		"a task that ran no container": func(t *testing.T, c *Capacity) {
			p := mustReserve(t, c)
			c.markUsed(p)
			a, _ := c.admit(true, small)
			a.Done()
			wantReserved(t, c, "done", amount{})
			c.release(p)
		},
		"a task delivered but never admitted": func(t *testing.T, c *Capacity) {
			p := mustReserve(t, c)
			c.markUsed(p)
			wantReserved(t, c, "delivered", testCeiling)
			c.release(p)
		},
		"a permit released before its task's container is removed": func(t *testing.T, c *Capacity) {
			p := mustReserve(t, c)
			c.markUsed(p)
			a, _ := c.admit(true, small)
			a.ContainerCreated("c1")
			c.release(p)
			wantReserved(t, c, "release before removal", small)
			a.ContainerRemoved(nil)
			a.Done()
		},
		"two permits released in the other order": func(t *testing.T, c *Capacity) {
			never, ran := mustReserve(t, c), mustReserve(t, c)
			c.markUsed(never)
			c.markUsed(ran)
			a, _ := c.admit(true, small)
			// The never-admitted permit is released first, while the admitted
			// one's task still runs: the total may over-hold, never under-hold.
			c.release(never)
			if got := c.reservedNow(); !small.within(got) {
				t.Fatalf("under-held: %s reserved while %s runs", got, small)
			}
			a.Done()
			c.release(ran)
		},
		"a container whose removal failed, reaped later": func(t *testing.T, c *Capacity) {
			p := mustReserve(t, c)
			c.markUsed(p)
			a, _ := c.admit(true, small)
			a.ContainerCreated("c1")
			a.ContainerRemoved(removeFailed)
			a.Done()
			c.release(p)
			wantReserved(t, c, "a failed removal", small)
			c.reap(context.Background(), func(context.Context, ContainerID) error { return removeFailed })
			wantReserved(t, c, "a reap that failed again", small)
			c.reap(context.Background(), func(context.Context, ContainerID) error { return nil })
		},
		"a container never reported removed": func(t *testing.T, c *Capacity) {
			p := mustReserve(t, c)
			c.markUsed(p)
			a, _ := c.admit(true, small)
			a.ContainerCreated("c1")
			a.Done()
			c.release(p)
			wantReserved(t, c, "done without a removal", small)
			c.reap(context.Background(), func(context.Context, ContainerID) error { return nil })
		},
		"a pinned member over capacity": func(t *testing.T, c *Capacity) {
			p, q := mustReserve(t, c), mustReserve(t, c)
			member, _ := c.admit(false, amount{milli: 3000, bytes: 1 << 30})
			if c.reservedNow().within(testLimit) {
				t.Fatal("a pinned member was not counted")
			}
			c.release(p)
			c.release(q)
			if c.tryReserve() != nil {
				t.Fatal("a poll was granted while a pinned member left less than a cap's worth free")
			}
			member.ContainerCreated("m1")
			member.ContainerRemoved(nil)
			member.Done()
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := newTestCapacity()
			steps(t, c)
			wantReserved(t, c, "the end", amount{})
			if c.issued != 0 || c.unclaimed != 0 || c.claimed != 0 || len(c.unremoved) != 0 || c.pinned != (amount{}) {
				t.Fatalf("the ledger did not return to rest: issued %d unclaimed %d claimed %d unremoved %d pinned %s",
					c.issued, c.unclaimed, c.claimed, len(c.unremoved), c.pinned)
			}
		})
	}
}

func TestCapacity_ARequestLargerThanItsPollIsRefusedLoudly(t *testing.T) {
	c := newTestCapacity()
	p, q := mustReserve(t, c), mustReserve(t, c)
	c.markUsed(p)
	_, err := c.admit(true, amount{milli: 3000, bytes: 1 << 30})
	var failure *Failure
	if !errors.As(err, &failure) || failure.Reason != ReasonResourceAdmissionMismatch {
		t.Fatalf("admit = %v, want %s", err, ReasonResourceAdmissionMismatch)
	}
	if got := CategoryFor(ReasonResourceAdmissionMismatch); got != CategoryContractViolation {
		t.Fatalf("%s is %s, want a ContractViolation", ReasonResourceAdmissionMismatch, got)
	}
	c.release(p)
	c.release(q)
	wantReserved(t, c, "the refusal and both releases", amount{})
	// The same request fits when free capacity covers the difference.
	r := mustReserve(t, c)
	c.markUsed(r)
	if _, err := c.admit(true, amount{milli: 3000, bytes: 1 << 30}); err != nil {
		t.Fatalf("a larger request with room to spare was refused: %v", err)
	}
}

func TestCapacity_SeededOrphansHoldTheirShareUntilReaped(t *testing.T) {
	c := newTestCapacity()
	c.seed("orphan-1")
	c.seed("orphan-1")
	wantReserved(t, c, "one orphan seeded twice", testCeiling)
	removed := 0
	c.reap(context.Background(), func(context.Context, ContainerID) error { removed++; return nil })
	if removed != 1 || c.pendingRemovals() != 0 {
		t.Fatalf("reap removed %d, %d pending", removed, c.pendingRemovals())
	}
	wantReserved(t, c, "the reap", amount{})
}

func TestCapacity_AWaitingPollIsGrantedWhenCapacityFrees(t *testing.T) {
	c := newTestCapacity()
	first, second := mustReserve(t, c), mustReserve(t, c)
	granted := make(chan *worker.SlotPermit, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		permit, _ := c.reserve(ctx)
		granted <- permit
	}()
	c.release(first)
	if permit := <-granted; permit == nil {
		t.Fatal("the waiting poll was never granted once a cap's worth was free")
	}
	c.release(second)
}

func TestCapacity_ACordonStillRefusesFirst(t *testing.T) {
	cordon := NewCordon()
	cordon.Set("patch window", time.Now())
	supplier := &cordonSlotSupplier{inner: &capacitySlotSupplier{c: newTestCapacity()}, cordon: cordon}
	if supplier.TryReserveSlot(nil) != nil {
		t.Fatal("a cordoned Orker with free capacity granted a poll")
	}
}

func TestParseCapacity_ReadsEitherDimensionAlone(t *testing.T) {
	for _, tc := range []struct {
		cpu, memory string
		want        amount
		declared    bool
		bad         bool
	}{
		{"", "", amount{}, false, false},
		{"16", "", amount{milli: 16000}, true, false},
		{"", "64Gi", amount{bytes: 64 << 30}, true, false},
		{"15500m", "1Gi", amount{milli: 15500, bytes: 1 << 30}, true, false},
		{"lots", "", amount{}, false, true},
		{"", "64GB", amount{}, false, true},
		{"0", "", amount{}, false, true},
	} {
		got, declared, err := parseCapacity(tc.cpu, tc.memory)
		if (err != nil) != tc.bad || declared != tc.declared || (!tc.bad && got != tc.want) {
			t.Errorf("parseCapacity(%q, %q) = %v, %v, %v", tc.cpu, tc.memory, got, declared, err)
		}
	}
}

func TestActivities_RefuseACapacityTheCapDoesNotFit(t *testing.T) {
	policy := DefaultSecurityPolicy() // a cap of 8 cpu, 16Gi
	for name, opts := range map[string]Options{
		"cpu below the cap":    {CapacityCPU: "4", TaskQueue: "q"},
		"memory below the cap": {CapacityMemory: "8Gi", TaskQueue: "q"},
		"no task queue":        {CapacityCPU: "16"},
		"unreadable":           {CapacityCPU: "16 cores", TaskQueue: "q"},
	} {
		if _, err := newCapacityFor(policy, opts, 16); err == nil {
			t.Errorf("%s: a capacity was accepted", name)
		}
	}
	c, err := newCapacityFor(policy, Options{CapacityCPU: "16", CapacityMemory: "64Gi", TaskQueue: "q"}, 16)
	if err != nil || c == nil {
		t.Fatalf("a capacity the cap fits in = %v, %v", c, err)
	}
}

func TestDescribeOrker_ReportsCapacityAndTheRequestBounds(t *testing.T) {
	policy := DefaultSecurityPolicy()
	activities := NewActivitiesWithOptions(nil, Options{
		Policy: &policy, Runtime: &layoutProbeRuntime{}, OrkerID: "o1",
		CapacityCPU: "16", CapacityMemory: "64Gi", TaskQueue: "q",
	})
	described, err := activities.DescribeOrker(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := described.GetCapacity(); got.GetCpu() != 16 || got.GetMemoryBytes() != 64<<30 {
		t.Errorf("capacity = %v", got)
	}
	if got := described.GetMaxRequest(); got.GetCpu() != 8 || got.GetMemoryBytes() != 16<<30 {
		t.Errorf("max_request = %v", got)
	}
	if got := described.GetDefaultRequest(); got.GetCpu() != 1 || got.GetMemoryBytes() != 1<<30 {
		t.Errorf("default_request = %v", got)
	}
	if described.GetReserved() == nil {
		t.Error("reserved is unset on an Orker declaring a capacity")
	}
	plain := NewActivitiesWithOptions(nil, Options{Policy: &policy, Runtime: &layoutProbeRuntime{}, OrkerID: "o2"})
	d, _ := plain.DescribeOrker(context.Background())
	if d.GetCapacity() != nil || d.GetReserved() != nil || d.GetMaxRequest() == nil {
		t.Errorf("an Orker declaring no capacity reports %v / %v / %v", d.GetCapacity(), d.GetReserved(), d.GetMaxRequest())
	}
}
