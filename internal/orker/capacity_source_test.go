package orker

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/worker"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// The ledger of an Orker that hosts event sources: a second channel, with its
// own ceiling and slots, over the one total.

var sourceCeiling = amount{milli: 500, bytes: 256 << 20}

func newSourceCapacity(instances int) *Capacity {
	c := newTestCapacity()
	c.source = &channelLedger{ceiling: sourceCeiling, slots: instances, polls: 1}
	return c
}

func mustPoll(t *testing.T, c *Capacity, ch *channelLedger) *worker.SlotPermit {
	t.Helper()
	p := c.tryReserveOn(ch)
	if p == nil {
		t.Fatalf("no poll granted with %s reserved", c.reservedNow())
	}
	return p
}

// TestCapacity_WithoutASourceChannelNothingIsHeldForOne: an Orker that hosts no
// source admits tasks to the full total, as it did, and no activity is of a
// source channel.
func TestCapacity_WithoutASourceChannelNothingIsHeldForOne(t *testing.T) {
	policy := DefaultSecurityPolicy()
	a := NewActivitiesWithOptions(nil, Options{Policy: &policy, Runtime: &layoutProbeRuntime{}, OrkerID: "o1",
		CapacityCPU: "16", CapacityMemory: "32Gi", TaskQueue: "q", SourceQueue: "ork-source-main", MaxConcurrent: 64,
		SourceHost: SourceHostSettings{MaxInstances: 0, MaxRequestCPU: "500m", MaxRequestMemory: "256Mi"}})
	must.OK(t, a.capacityErr)
	c := a.capacity
	if c.source != nil || a.polledOn("ork-source-main") != nil || a.polledOn("") != nil || a.polledOn("a-pin's") != nil {
		t.Fatal("an Orker that hosts no source has a source channel")
	}
	if a.polledOn("q") != &c.channelLedger {
		t.Fatal("the pool's channel is not the pool's")
	}
	whole := amount{milli: 8000, bytes: 16 << 30}
	for range 2 {
		c.markUsed(mustPoll(t, c, &c.channelLedger))
		_, err := c.admit(a.polledOn("q"), whole)
		must.OK(t, err)
	}
	wantReserved(t, c, "two tasks of the whole cap", whole.add(whole))
	if c.reservedNow() != c.limit || c.tryReserve() != nil {
		t.Fatalf("reserved %s of %s", c.reservedNow(), c.limit)
	}
}

// TestCapacity_TwoChannelsOneTotal: each channel's polls reserve its own ceiling
// against the one total, and each is bounded by its own slots.
func TestCapacity_TwoChannelsOneTotal(t *testing.T) {
	c := newSourceCapacity(2)
	first := mustPoll(t, c, c.source)
	wantReserved(t, c, "a source poll", sourceCeiling)
	if c.tryReserveOn(c.source) != nil {
		t.Fatal("a second poll of the source channel while one waits")
	}
	mustPoll(t, c, &c.channelLedger)
	wantReserved(t, c, "and a task's", sourceCeiling.add(testCeiling))
	if c.tryReserve() != nil {
		t.Fatal("a task's poll was granted with no room for the cap beside an instance's")
	}
	c.markUsed(first)
	second := mustPoll(t, c, c.source)
	c.markUsed(second)
	if c.tryReserveOn(c.source) != nil {
		t.Fatal("a source poll past max_instances")
	}
	c.release(first)
	mustPoll(t, c, c.source)
	if c.issued != 1 || c.source.issued != 2 {
		t.Fatalf("permits %d and %d", c.issued, c.source.issued)
	}
}

// TestCapacity_AnInstanceIsTrimmedOnItsOwnChannel: an instance is trimmed from
// its channel's ceiling whatever the pool's polls are doing, and a task from the
// pool's; each permit's release gives back its own.
func TestCapacity_AnInstanceIsTrimmedOnItsOwnChannel(t *testing.T) {
	c := newSourceCapacity(2)
	task, instance := mustPoll(t, c, &c.channelLedger), mustPoll(t, c, c.source)
	c.markUsed(instance)
	asked := amount{milli: 200, bytes: 128 << 20}
	held, err := c.admit(c.source, asked)
	must.OK(t, err)
	wantReserved(t, c, "an instance admitted beside a waiting poll", testCeiling.add(asked))
	c.markUsed(task)
	ran, err := c.admit(&c.channelLedger, small)
	must.OK(t, err)
	wantReserved(t, c, "and a task", small.add(asked))
	ran.Done()
	c.release(task)
	wantReserved(t, c, "the task done", asked)
	held.ContainerCreated("c1")
	held.Done()
	c.release(instance)
	wantReserved(t, c, "an instance whose container is not gone", asked)
	c.reap(t.Context(), func(context.Context, ContainerID) error { return nil })
	wantReserved(t, c, "its container gone", amount{})
	if c.channelLedger != (channelLedger{ceiling: testCeiling, slots: 16}) || *c.source != (channelLedger{ceiling: sourceCeiling, slots: 2, polls: 1}) {
		t.Fatalf("at rest: %+v, %+v", c.channelLedger, *c.source)
	}
	// Delivered and never admitted, on either channel.
	for _, ch := range []*channelLedger{c.source, &c.channelLedger} {
		p := mustPoll(t, c, ch)
		c.markUsed(p)
		wantReserved(t, c, "delivered", ch.ceiling)
		c.release(p)
		wantReserved(t, c, "released", amount{})
	}
}

// TestCapacity_AnInstanceOverItsPollIsRefusedOnlyWithNoRoom: what an instance
// asks is held to its own channel's ceiling, never the per-task cap.
func TestCapacity_AnInstanceOverItsPollIsRefusedOnlyWithNoRoom(t *testing.T) {
	over := amount{milli: 1000, bytes: 1 << 30}
	c := newSourceCapacity(2)
	c.markUsed(mustPoll(t, c, c.source))
	a, err := c.admit(c.source, over)
	must.OK(t, err)
	a.Done()
	c = newSourceCapacity(2)
	p := mustPoll(t, c, &c.channelLedger)
	c.markUsed(p)
	_, err = c.admit(&c.channelLedger, testCeiling)
	must.OK(t, err)
	_, err = c.admit(nil, amount{milli: 1500, bytes: 0})
	must.OK(t, err)
	c.markUsed(mustPoll(t, c, c.source))
	if _, err := c.admit(c.source, over); !isReason(err, ReasonResourceAdmissionMismatch) || !strings.Contains(err.Error(), "0.5 cpu") {
		t.Fatalf("an instance with no room: %v", err)
	}
	unknown := newSourceCapacity(2)
	unknown.markUsed(mustPoll(t, unknown, unknown.source))
	u, err := unknown.admitUnknown(unknown.source)
	must.OK(t, err)
	wantReserved(t, unknown, "an instance not read yet", sourceCeiling)
	_, err = unknown.admit(nil, amount{milli: 3500})
	must.OK(t, err)
	if err := u.Resolve(amount{milli: 600}); !isReason(err, ReasonResourceAdmissionMismatch) {
		t.Fatalf("resolved past the total: %v", err)
	}
	must.OK(t, u.Resolve(amount{milli: 100}))
	wantReserved(t, unknown, "read", amount{milli: 3600})
}

// TestCapacity_AWaitingPollOfEitherChannelIsGrantedWhenRoomReturns: room one
// channel gives back is the other's too.
func TestCapacity_AWaitingPollOfEitherChannelIsGrantedWhenRoomReturns(t *testing.T) {
	c := newSourceCapacity(1)
	one, two := mustPoll(t, c, &c.channelLedger), mustPoll(t, c, &c.channelLedger)
	granted := make(chan error, 1)
	go func() {
		_, err := c.reserve(t.Context(), c.source, nil)
		granted <- err
	}()
	for waiting := 0; waiting == 0; time.Sleep(time.Millisecond) {
		c.mu.Lock()
		waiting = c.waiting
		c.mu.Unlock()
	}
	c.release(one)
	must.OK(t, <-granted)
	if c.tryReserve() != nil {
		t.Fatal("the total was granted twice")
	}
	c.release(two)
	mustPoll(t, c, &c.channelLedger)
}

// TestCapacity_AUnitProbeHoldsBackOnlyThePool: a due unit probe waits for the
// polls that reserve units; an instance has none.
func TestCapacity_AUnitProbeHoldsBackOnlyThePool(t *testing.T) {
	c := newSourceCapacity(1)
	c.useUnits("gpu", 2, 1, nil)
	c.mu.Lock()
	c.units.probeDue = 1
	c.mu.Unlock()
	if c.tryReserve() != nil {
		t.Fatal("a task's poll was granted past a due probe")
	}
	mustPoll(t, c, c.source)
}

// TestActivities_ASourceHostsCapacity: a source host's ledger has the source
// channel its settings say, and a capacity that could not hold a task's poll
// beside an instance's is refused at boot, naming both.
func TestActivities_ASourceHostsCapacity(t *testing.T) {
	policy := DefaultSecurityPolicy() // a cap of 8 cpu, 16Gi
	host := SourceHostSettings{MaxInstances: 3, MaxRequestCPU: "500m", MaxRequestMemory: "256Mi"}
	opts := Options{CapacityCPU: "8500m", CapacityMemory: "17Gi", TaskQueue: "q", SourceQueue: "s", SourceHost: host}
	c, err := newCapacityFor(policy, opts, 16)
	must.OK(t, err)
	if c.source == nil || *c.source != (channelLedger{ceiling: sourceCeiling, slots: 3, polls: 1}) {
		t.Fatalf("the source channel: %+v", c.source)
	}
	for name, spoil := range map[string]func(*Options){
		"cpu":               func(o *Options) { o.CapacityCPU = "8400m" },
		"memory":            func(o *Options) { o.CapacityMemory = "16Gi" },
		"no source channel": func(o *Options) { o.SourceQueue = "" },
		"the pool's own":    func(o *Options) { o.SourceQueue = o.TaskQueue },
		"unreadable":        func(o *Options) { o.SourceHost.MaxRequestCPU = "half" },
	} {
		bad := opts
		spoil(&bad)
		if _, err := newCapacityFor(policy, bad, 16); err == nil || !strings.Contains(err.Error(), map[bool]string{true: SourceHostMaxRequestCPUVar, false: "Options.SourceQueue"}[name != "no source channel" && name != "the pool's own"]) {
			t.Errorf("%s: %v", name, err)
		}
	}
	// No capacity declared: nothing to hold the cap to.
	if c, err := newCapacityFor(policy, Options{TaskQueue: "q", SourceQueue: "s", SourceHost: host}, 16); c != nil || err != nil {
		t.Errorf("no capacity declared: %v, %v", c, err)
	}
	a := NewActivitiesWithOptions(nil, Options{Policy: &policy, Runtime: &layoutProbeRuntime{}, OrkerID: "o1", CapacityCPU: "16", TaskQueue: "q",
		SourceQueue: "s", SourceHost: host})
	if a.polledOn("s") != a.capacity.source || a.polledOn("s") == nil || a.polledOn("q") != &a.capacity.channelLedger {
		t.Error("a source channel's activity is not the source channel's")
	}
}

// TestSourceTuner: the source worker's activity slots are max_instances, over
// the ledger where a capacity is declared and a fixed count where none is, and
// an operator's cordon stops its polls as it stops the pool's.
func TestSourceTuner(t *testing.T) {
	policy := DefaultSecurityPolicy()
	host := SourceHostSettings{MaxInstances: 2, MaxRequestCPU: "500m", MaxRequestMemory: "256Mi"}
	for name, capacity := range map[string]string{"a declared capacity": "16", "none declared": ""} {
		a := NewActivitiesWithOptions(nil, Options{Policy: &policy, Runtime: &layoutProbeRuntime{}, OrkerID: "o1", CapacityCPU: capacity, TaskQueue: "q",
			SourceQueue: "s", SourceHost: host})
		tuner, err := a.SourceWorkerTuner()
		must.OK(t, err)
		slots := tuner.GetActivityTaskSlotSupplier()
		if slots.MaxSlots() != 2 {
			t.Errorf("%s: %d slots", name, slots.MaxSlots())
		}
		one := slots.TryReserveSlot(nil)
		slots.MarkSlotUsed(markUsedInfo{permit: one})
		two := slots.TryReserveSlot(nil)
		slots.MarkSlotUsed(markUsedInfo{permit: two})
		if one == nil || two == nil || slots.TryReserveSlot(nil) != nil {
			t.Errorf("%s: slots past max_instances, or none", name)
		}
		a.cordon.Set("maintenance", time.Now())
		slots.ReleaseSlot(releaseInfo{permit: one})
		if slots.TryReserveSlot(nil) != nil {
			t.Errorf("%s: a poll while cordoned", name)
		}
	}
}

// releaseInfo is a release of permit; the suppliers read nothing else of one.
type releaseInfo struct {
	worker.SlotReleaseInfo
	permit *worker.SlotPermit
}

func (r releaseInfo) Permit() *worker.SlotPermit { return r.permit }

type markUsedInfo struct {
	worker.SlotMarkUsedInfo
	permit *worker.SlotPermit
}

func (m markUsedInfo) Permit() *worker.SlotPermit { return m.permit }

// TestInterceptor_AnUnknownActivityOfTheSourceChannelKeepsItsPoll: what runs on
// the source channel that this Orker cannot size holds all its poll reserved
// until it is done; on the pool's, which carries only DescribeOrker beside
// tasks, nothing.
func TestInterceptor_AnUnknownActivityOfTheSourceChannelKeepsItsPoll(t *testing.T) {
	for queue, want := range map[string]amount{"s": sourceCeiling, "q": {}, "another": {}} {
		a := &Activities{capacity: newSourceCapacity(2), taskQueue: "q", sourceQueue: "s"}
		if ch := a.polledOn(queue); ch != nil {
			a.capacity.markUsed(mustPoll(t, a.capacity, ch))
		}
		admission, err := a.admitUnsized(queue)
		must.OK(t, err)
		wantReserved(t, a.capacity, queue, want)
		admission.Done()
		wantReserved(t, a.capacity, queue+" done", amount{})
	}
}

// TestBoot_WarnsThatInstancesCanFillTheCapacity: a source host whose instances,
// all of the largest size, would leave no room for a task's poll says so at
// boot, with the sizes; one that always has that room, or declares no capacity,
// does not.
func TestBoot_WarnsThatInstancesCanFillTheCapacity(t *testing.T) {
	for capacity, warns := range map[string]bool{"9": true, "9500m": false, "": false} {
		logs := &lockedBuffer{}
		a := NewActivitiesWithOptions(nil, Options{Runtime: &reapRecorder{labelled: map[string]int{}}, WorkRoot: t.TempDir(), CapacityCPU: capacity,
			TaskQueue: "q", SourceQueue: "s", Logger: jsonLogTo(logs),
			SourceHost: SourceHostSettings{MaxInstances: 3, MaxRequestCPU: "500m", MaxRequestMemory: "256Mi"}})
		t.Cleanup(a.Close)
		must.OK(t, a.Boot(t.Context()))
		record := withAttr(logRecords(t, logs), "instances_at_most")
		if (record != nil) != warns || record != nil && record.level() != "WARN" {
			t.Errorf("capacity %q: %v", capacity, record)
		}
	}
}
