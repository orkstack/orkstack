package orker

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// gpuLedger is a capacity with no cpu or memory limit, max_concurrent slots,
// and n units of one gpu profile whose ceiling is min(maxGPU, n). Every health
// report (the usable count) is appended to the returned slice.
func gpuLedger(t *testing.T, slots, n, maxGPU int) (*Capacity, *[]int) {
	t.Helper()
	c := newCapacity(amount{}, amount{milli: 100, bytes: 1 << 20}, slots)
	var healths []int
	c.useUnits("amd", n, maxGPU, func(usable, _ int) { healths = append(healths, usable) })
	return c, &healths
}

func gpuReq(n int64) amount { return amount{milli: 100, bytes: 1 << 20, gpu: n} }

func isReason(err error, reason string) bool {
	var f *Failure
	return errors.As(err, &f) && f.Reason == reason
}

// pollTask grants a poll, delivers a task on it and admits request: what the
// SDK and the dispatcher do for one polled task. The returned func releases
// the permit, as the SDK does once the activity returns.
func pollTask(t *testing.T, c *Capacity, request amount) (*Admission, func()) {
	t.Helper()
	permit := c.tryReserve()
	if permit == nil {
		t.Fatal("the poll was not granted")
	}
	c.markUsed(permit)
	a, err := c.admit(true, request)
	if err != nil {
		t.Fatalf("admit: %v", err)
	}
	return a, func() { c.release(permit) }
}

func wantGPUReserved(t *testing.T, c *Capacity, when string, want int64) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reserved.gpu != want {
		t.Fatalf("%s: gpu reserved = %d, want %d", when, c.reserved.gpu, want)
	}
}

func waitFor(t *testing.T, ctx context.Context, cond func() bool) {
	t.Helper()
	for !cond() {
		select {
		case <-ctx.Done():
			t.Fatal("condition never held")
		case <-time.After(time.Millisecond):
		}
	}
}

// TestCapacity_GPUZeroAdmitsNone: a 0 gpu limit is no gpu at all, unlike cpu
// and memory, where 0 is unbounded.
func TestCapacity_GPUZeroAdmitsNone(t *testing.T) {
	plain := newCapacity(amount{}, amount{milli: 100, bytes: 1 << 20}, 2)
	permit := plain.tryReserve()
	if permit == nil {
		t.Fatal("a cpu-only poll was not granted under an unbounded limit")
	}
	plain.markUsed(permit)
	if _, err := plain.admit(true, gpuReq(1)); !isReason(err, ReasonResourceAdmissionMismatch) {
		t.Fatalf("admit gpu 1 on an Orker with no units = %v, want %s", err, ReasonResourceAdmissionMismatch)
	}
	if err := plain.bind(&Admission{c: plain, held: gpuReq(1)}); !isReason(err, ReasonGPUUnitsUnavailable) {
		t.Fatalf("bind on an Orker with no units = %v, want %s", err, ReasonGPUUnitsUnavailable)
	}
}

// TestCapacity_GPUCeilingFixedAcrossHealth: a unit that leaves and returns
// while polls are outstanding moves only the limit; every permit is released
// with what it was granted with, and no admission mismatches.
func TestCapacity_GPUCeilingFixedAcrossHealth(t *testing.T) {
	c, healths := gpuLedger(t, 4, 4, 2)
	if got := c.ceiling.gpu; got != 2 {
		t.Fatalf("ceiling = %d, want min(max_gpu 2, 4 units)", got)
	}
	first := c.tryReserve() // holds 2, no task yet
	if first == nil {
		t.Fatal("first poll not granted")
	}
	p, err := c.probeUnit(context.Background(), 3)
	if err != nil {
		t.Fatalf("probe unit 3: %v", err)
	}
	c.endProbe(p, false) // unit 3 fails: usable 3
	if c.ceiling.gpu != 2 || c.limit.gpu != 3 {
		t.Fatalf("after a fault: ceiling %d limit %d, want 2 and 3", c.ceiling.gpu, c.limit.gpu)
	}
	if c.tryReserve() != nil {
		t.Fatal("a second poll was granted with 1 usable unit free for a ceiling of 2")
	}
	c.release(first)
	for range 3 { // three passes bring it back
		p, err := c.probeUnit(context.Background(), 3)
		if err != nil {
			t.Fatalf("recovery probe: %v", err)
		}
		c.endProbe(p, true)
	}
	if c.limit.gpu != 4 {
		t.Fatalf("limit after recovery = %d, want 4", c.limit.gpu)
	}
	a, done := pollTask(t, c, gpuReq(2))
	if err := c.bind(a); err != nil {
		t.Fatalf("bind after recovery: %v", err)
	}
	a.ContainerCreated("c1")
	a.ContainerRemoved(nil)
	done()
	wantGPUReserved(t, c, "everything released", 0)
	if want := []int{3, 4}; !slices.Equal(*healths, want) {
		t.Fatalf("health reports = %v, want %v", *healths, want)
	}
}

// TestCapacity_BindTakesLowestFreeUsable: bind is all or nothing over the
// lowest free usable indices, and the end of each admission frees exactly
// those.
func TestCapacity_BindTakesLowestFreeUsable(t *testing.T) {
	c, _ := gpuLedger(t, 4, 4, 2)
	p, _ := c.probeUnit(context.Background(), 0)
	c.endProbe(p, false) // unit 0 out
	a, doneA := pollTask(t, c, gpuReq(1))
	if err := c.bind(a); err != nil {
		t.Fatal(err)
	}
	b, doneB := pollTask(t, c, gpuReq(2))
	if err := c.bind(b); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(a.Units(), []int{1}) || !slices.Equal(b.Units(), []int{2, 3}) {
		t.Fatalf("units = %v and %v, want [1] and [2 3]", a.Units(), b.Units())
	}
	a.Done()
	doneA()
	b.Done()
	doneB()
	wantGPUReserved(t, c, "done", 0)
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, o := range c.units.owner {
		if o != nil {
			t.Fatalf("unit %d still bound after every admission ended", i)
		}
	}
}

// TestCapacity_ProbeVsPolledBindOneUnit: one unit, two pollers and a probe
// racing; a poll is never granted while the probe holds the unit, so bind
// never falls short.
func TestCapacity_ProbeVsPolledBindOneUnit(t *testing.T) {
	c, _ := gpuLedger(t, 2, 1, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for range 2 {
		wg.Go(func() {
			for range 50 {
				permit, err := c.reserve(ctx, nil)
				if err != nil {
					errs <- err
					return
				}
				c.markUsed(permit)
				a, err := c.admit(true, gpuReq(1))
				if err != nil {
					errs <- err
					c.release(permit)
					return
				}
				if err := c.bind(a); err != nil {
					errs <- err
				}
				a.ContainerCreated("x")
				a.ContainerRemoved(nil)
				c.release(permit)
			}
		})
	}
	wg.Go(func() {
		for range 50 {
			p, err := c.probeUnit(ctx, 0)
			if errors.Is(err, errProbeSkipped) {
				continue
			}
			if err != nil {
				errs <- err
				return
			}
			c.endProbe(p, true)
		}
	})
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	wantGPUReserved(t, c, "after the race", 0)
}

// TestCapacity_ProbeSkipsABoundUnit: a probe never waits for a bound unit.
func TestCapacity_ProbeSkipsABoundUnit(t *testing.T) {
	c, _ := gpuLedger(t, 2, 2, 1)
	a, done := pollTask(t, c, gpuReq(1))
	defer done()
	if err := c.bind(a); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := c.probeUnit(ctx, 0); !errors.Is(err, errProbeSkipped) {
		t.Fatalf("probe of a bound unit = %v, want skipped", err)
	}
	a.Done()
}

// TestCapacity_RecoveryProbeAtUsableZero: with no usable unit the limit is 0,
// yet a recovery probe still runs: it takes no count.
func TestCapacity_RecoveryProbeAtUsableZero(t *testing.T) {
	c, healths := gpuLedger(t, 1, 1, 1)
	p, err := c.probeUnit(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	c.endProbe(p, false)
	if c.limit.gpu != 0 {
		t.Fatalf("limit = %d, want 0", c.limit.gpu)
	}
	for range 3 {
		p, err := c.probeUnit(context.Background(), 0)
		if err != nil {
			t.Fatalf("recovery probe at usable 0: %v", err)
		}
		wantGPUReserved(t, c, "a recovery probe", 0)
		c.endProbe(p, true)
	}
	if c.limit.gpu != 1 {
		t.Fatalf("limit after three passes = %d, want 1", c.limit.gpu)
	}
	if want := []int{0, 1}; !slices.Equal(*healths, want) {
		t.Fatalf("health reports = %v, want %v", *healths, want)
	}
}

// TestCapacity_RecoveryNeedsConsecutivePasses: a failure during recovery
// starts the count again.
func TestCapacity_RecoveryNeedsConsecutivePasses(t *testing.T) {
	c, _ := gpuLedger(t, 1, 1, 1)
	for _, healthy := range []bool{false, true, true, false, true, true} {
		p, err := c.probeUnit(context.Background(), 0)
		if err != nil {
			t.Fatal(err)
		}
		c.endProbe(p, healthy)
	}
	if c.limit.gpu != 0 {
		t.Fatalf("limit = %d after an interrupted recovery, want 0", c.limit.gpu)
	}
}

// TestCapacity_ProbeDueDrainsUnclaimedPolls: a probe short of count only
// because of polls that carry no task stops new grants until they drain.
func TestCapacity_ProbeDueDrainsUnclaimedPolls(t *testing.T) {
	c, _ := gpuLedger(t, 2, 1, 1)
	permit := c.tryReserve() // the whole unit, no task yet
	if permit == nil {
		t.Fatal("poll not granted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got := make(chan error, 1)
	go func() {
		p, err := c.probeUnit(ctx, 0)
		if err == nil {
			c.endProbe(p, true)
		}
		got <- err
	}()
	waitFor(t, ctx, func() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.units.probeDue > 0 })
	if c.tryReserve() != nil {
		t.Fatal("a poll was granted while a probe was due")
	}
	c.release(permit) // the empty poll drains
	if err := <-got; err != nil {
		t.Fatalf("the due probe = %v, want it to run once the poll drained", err)
	}
	c.mu.Lock()
	due := c.units.probeDue
	c.mu.Unlock()
	if due != 0 {
		t.Fatalf("probeDue = %d after the probe, want 0", due)
	}
	if c.tryReserve() == nil {
		t.Fatal("no poll granted once the probe ended")
	}

	// Short because a task is bound: skipped, never due.
	c2, _ := gpuLedger(t, 2, 1, 1)
	a, done := pollTask(t, c2, gpuReq(1))
	defer done()
	if err := c2.bind(a); err != nil {
		t.Fatal(err)
	}
	if _, err := c2.probeUnit(ctx, 0); !errors.Is(err, errProbeSkipped) {
		t.Fatalf("probe beside a bound task = %v, want skipped", err)
	}
	a.Done()
}

// TestCapacity_ProbeDueCancelled: a due probe whose context ends clears its
// mark, so grants resume.
func TestCapacity_ProbeDueCancelled(t *testing.T) {
	c, _ := gpuLedger(t, 2, 1, 1)
	permit := c.tryReserve()
	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan error, 1)
	go func() { _, err := c.probeUnit(ctx, 0); got <- err }()
	waitFor(t, context.Background(), func() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.units.probeDue > 0 })
	cancel()
	if err := <-got; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled due probe = %v", err)
	}
	c.release(permit)
	if c.tryReserve() == nil {
		t.Fatal("grants did not resume after the due probe was cancelled")
	}
}

// TestCapacity_SeedOnceByContainerID: the by-id pass seeds cpu and memory
// only; the lock pass merges the indices into the same entry; one removal
// frees both.
func TestCapacity_SeedOnceByContainerID(t *testing.T) {
	c, _ := gpuLedger(t, 2, 2, 1)
	c.seed("left")
	c.seed("left") // twice is once
	c.seedUnits("left", []int{1})
	c.seedUnits("left", []int{1})
	c.mu.Lock()
	if c.reserved.milli != 100 || c.reserved.gpu != 1 || c.units.owner[1] == nil {
		t.Fatalf("seeded: reserved %v, unit 1 owner %v", c.reserved, c.units.owner[1])
	}
	c.mu.Unlock()
	if failed := c.reap(context.Background(), func(context.Context, ContainerID) error { return nil }); len(failed) != 0 {
		t.Fatal(failed)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reserved != (amount{}) || c.units.owner[1] != nil {
		t.Fatalf("after removal: reserved %v, unit 1 owner %v", c.reserved, c.units.owner[1])
	}
}

// TestCapacity_SeedExactIndicesOnFailedRemove: a container the lock pass
// could not remove holds exactly its indices, so bind skips them.
func TestCapacity_SeedExactIndicesOnFailedRemove(t *testing.T) {
	c, _ := gpuLedger(t, 2, 3, 1)
	c.seedUnits("stuck", []int{1})
	a, done := pollTask(t, c, gpuReq(1))
	defer done()
	if err := c.bind(a); err != nil {
		t.Fatal(err)
	}
	b, doneB := pollTask(t, c, gpuReq(1))
	defer doneB()
	if err := c.bind(b); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(a.Units(), []int{0}) || !slices.Equal(b.Units(), []int{2}) {
		t.Fatalf("units %v and %v, want [0] and [2] around the seeded 1", a.Units(), b.Units())
	}
	a.Done()
	b.Done()
}

// TestCapacity_FailedRemovalKeepsUnits: a container whose removal failed
// keeps its units until a reap removes it.
func TestCapacity_FailedRemovalKeepsUnits(t *testing.T) {
	c, _ := gpuLedger(t, 2, 1, 1)
	a, done := pollTask(t, c, gpuReq(1))
	if err := c.bind(a); err != nil {
		t.Fatal(err)
	}
	a.ContainerCreated("c1")
	a.ContainerRemoved(errors.New("daemon busy"))
	done()
	if c.tryReserve() != nil {
		t.Fatal("a poll was granted while an unremoved container holds the only unit")
	}
	c.reap(context.Background(), func(context.Context, ContainerID) error { return nil })
	wantGPUReserved(t, c, "after the reap", 0)
	if c.tryReserve() == nil {
		t.Fatal("no poll granted once the container was removed")
	}
}

// TestProfileState_UnitsBelowCeilingNamed: while fewer units are usable than
// the per-task cap every poll reserves, the Orker polls for no task, and the
// profile says why (UNITS_BELOW_CEILING); with none usable it also holds
// ALL_UNITS_UNHEALTHY. Each clears in the step that brings the units back.
func TestProfileState_UnitsBelowCeilingNamed(t *testing.T) {
	const (
		below = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_UNITS_BELOW_CEILING
		none  = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_ALL_UNITS_UNHEALTHY
	)
	s := newProfileState(Profiles{}, "")
	c := newCapacity(amount{}, amount{milli: 100, bytes: 1 << 20}, 2)
	c.useUnits("amd", 2, 2, unitHealthReporter(s, "amd", func() time.Time { return time.Unix(0, 0) }))
	fail := func(i int, healthy bool) {
		t.Helper()
		p, err := c.probeUnit(context.Background(), i)
		if err != nil {
			t.Fatal(err)
		}
		c.endProbe(p, healthy)
	}
	fail(1, false)
	if !s.unavailableFor("amd", below) || s.unavailableFor("amd", none) {
		t.Fatal("one of two units out under a cap of 2: want UNITS_BELOW_CEILING only")
	}
	r := &orkstackv1.DescribeOrkerResult{}
	s.report(r)
	if got := r.GetProfileCordons(); len(got) != 1 || !strings.Contains(got[0].GetDetail(), "1 of 2") {
		t.Fatalf("reported %v, want one cordon naming 1 of 2 units", got)
	}
	fail(0, false)
	if !s.unavailableFor("amd", below) || !s.unavailableFor("amd", none) {
		t.Fatal("no unit usable: want both reasons")
	}
	for range recoveryPasses {
		fail(0, true)
	}
	if !s.unavailableFor("amd", below) || s.unavailableFor("amd", none) {
		t.Fatal("one unit back: ALL_UNITS_UNHEALTHY must clear, UNITS_BELOW_CEILING stand")
	}
	for range recoveryPasses {
		fail(1, true)
	}
	if s.unavailable("amd") {
		t.Fatal("every unit back: the profile is still unavailable")
	}
}
