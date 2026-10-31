package api_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

const capGiB = int64(1) << 30

// capacityFleet is a pool "build" whose one Orker is full (4 of 4 cpu
// reserved, a 2 cpu per-task cap) and a pool "gpu" whose one Orker has room.
func capacityFleet() *stubFleet {
	return &stubFleet{
		presence: []engine.OrkerPresence{
			{Identity: "orker-b1", LastSeen: fleetSeen},
			{Identity: "orker-g1", LastSeen: fleetSeen},
		},
		described: map[string]*orkstackv1.DescribeOrkerResult{
			"orker-b1": {OrkerId: "orker-b1", Pool: "build", MaxConcurrent: 16, RunningTasks: 2,
				Capacity:   &orkstackv1.ResourceQuantity{Cpu: 4, MemoryBytes: 16 * capGiB},
				Reserved:   &orkstackv1.ResourceQuantity{Cpu: 4, MemoryBytes: 2 * capGiB},
				MaxRequest: &orkstackv1.ResourceQuantity{Cpu: 2, MemoryBytes: 4 * capGiB}},
			"orker-g1": {OrkerId: "orker-g1", Pool: "gpu", MaxConcurrent: 16,
				Capacity:   &orkstackv1.ResourceQuantity{Cpu: 8},
				Reserved:   &orkstackv1.ResourceQuantity{Cpu: 2},
				MaxRequest: &orkstackv1.ResourceQuantity{Cpu: 2}},
		},
	}
}

func newCapacityInspectFixture(t *testing.T, run *orkstackv1.Run, live []*orkstackv1.LiveAttempt, fleet *stubFleet) *inspectFixture {
	t.Helper()
	reader := &fakeReader{runs: map[string]*orkstackv1.Run{run.GetId(): run}}
	liveReader := &liveFakeReader{fakeReader: reader, live: engine.LiveRun{Attempts: live}}
	fixture := newReaderFixtureWith(t, reader, func(cfg *api.Config) {
		cfg.Reader = liveReader
		cfg.Now = func() time.Time { return inspectNow }
		cfg.Fleet = fleet
	})
	return &inspectFixture{readerFixture: fixture, live: liveReader}
}

func waitingAttempt(taskID string) *orkstackv1.LiveAttempt {
	return &orkstackv1.LiveAttempt{TaskId: taskID, Attempt: 1,
		State:              orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_SCHEDULED,
		AttemptScheduledAt: timestamppb.New(inspectNow.Add(-2 * time.Minute))}
}

func waitingItem(taskID string, item int32) *orkstackv1.LiveAttempt {
	a := waitingAttempt(taskID)
	a.Item = &item
	return a
}

func routeTasksTo(run *orkstackv1.Run, pool string, taskIDs ...string) {
	for _, task := range run.GetTaskRuns() {
		for _, id := range taskIDs {
			if task.GetTaskId() == id {
				task.Pool = pool
			}
		}
	}
}

// TestInspectRun_AnAttemptWaitingOnAFullPoolSaysItWaitsForCapacity: every
// SCHEDULED attempt of a pool whose Orkers are all full carries the pool's
// capacity wait — a task's own attempt and a fan-out's items alike — while an
// attempt an Orker has, and one waiting on a pool with room, carry none. The
// inventory is read ONCE for the whole answer.
func TestInspectRun_AnAttemptWaitingOnAFullPoolSaysItWaitsForCapacity(t *testing.T) {
	run := inspectRun("ork-capacity-1", orkstackv1.Status_STATUS_RUNNING, "fetch", "build", "convert", "render")
	routeTasksTo(run, "build", "fetch", "build")
	// The fan-out's own row names the pool with room and its items the full
	// one: an item waits where its own row says.
	routeTasksTo(run, "gpu", "convert", "render")
	convert := run.GetTaskRuns()[2]
	convert.Each = &orkstackv1.TaskEach{}
	convert.Items = []*orkstackv1.TaskRun{
		{TaskId: "convert", Pool: "build", Status: orkstackv1.Status_STATUS_RUNNING},
		{TaskId: "convert", Pool: "build", Status: orkstackv1.Status_STATUS_RUNNING},
	}
	started := &orkstackv1.LiveAttempt{TaskId: "fetch", Attempt: 1, State: orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_STARTED}
	fleet := capacityFleet()
	f := newCapacityInspectFixture(t, run, []*orkstackv1.LiveAttempt{
		started, waitingAttempt("build"), waitingItem("convert", 0), waitingItem("convert", 1), waitingAttempt("render"),
	}, fleet)

	resp, err := f.inspect(t, &orkstackv1.InspectRunRequest{RunId: run.GetId()})
	if err != nil {
		t.Fatalf("InspectRun: %v", err)
	}
	want := &orkstackv1.CapacityWait{Pool: "build", Orkers: 1,
		Reserved: &orkstackv1.ResourceQuantity{Cpu: 4, MemoryBytes: 2 * capGiB},
		Capacity: &orkstackv1.ResourceQuantity{Cpu: 4, MemoryBytes: 16 * capGiB}}
	marked := 0
	for _, attempt := range resp.GetAttempts() {
		got := attempt.GetCapacityWait()
		switch {
		case attempt.GetTaskId() == "build" || attempt.GetTaskId() == "convert":
			if !proto.Equal(got, want) {
				t.Errorf("%s item %d: capacity_wait = %v, want %v", attempt.GetTaskId(), attempt.GetItem(), got, want)
			}
			marked++
		case got != nil:
			t.Errorf("%s: capacity_wait = %v, want none (started, or its pool has room)", attempt.GetTaskId(), got)
		}
	}
	if marked != 3 {
		t.Fatalf("%d attempts on the full pool were answered, want 3", marked)
	}
	if fleet.observeOrkers != 1 || len(fleet.describes) != 2 {
		t.Fatalf("inventory reads = %d, describes = %v; want one read and one describe per Orker",
			fleet.observeOrkers, fleet.describes)
	}
}

// TestInspectRun_NoAttemptWaitingForAnOrkerReadsNoInventory: an attempt an
// Orker has, one waiting out a retry's backoff, a pinned member waiting for
// its pin's host, and one scheduled too recently for a poll to have missed it
// are none of them asked about, so the answer costs the inventory nothing.
func TestInspectRun_NoAttemptWaitingForAnOrkerReadsNoInventory(t *testing.T) {
	run := inspectRun("ork-capacity-2", orkstackv1.Status_STATUS_RUNNING, "fetch", "retry", "pinned", "fresh")
	routeTasksTo(run, "build", "fetch", "retry", "pinned", "fresh")
	run.GetTaskRuns()[2].Pin = "workspace"
	backoff := waitingAttempt("retry")
	backoff.Attempt = 2
	backoff.NextAttemptAt = timestamppb.New(inspectNow.Add(30 * time.Second))
	fresh := waitingAttempt("fresh")
	fresh.AttemptScheduledAt = timestamppb.New(inspectNow.Add(-time.Second))
	fleet := capacityFleet()
	f := newCapacityInspectFixture(t, run, []*orkstackv1.LiveAttempt{
		{TaskId: "fetch", Attempt: 1, State: orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_STARTED},
		backoff,
		waitingAttempt("pinned"),
		fresh,
	}, fleet)

	resp, err := f.inspect(t, &orkstackv1.InspectRunRequest{RunId: run.GetId()})
	if err != nil {
		t.Fatalf("InspectRun: %v", err)
	}
	if len(resp.GetAttempts()) != 4 {
		t.Fatalf("attempts = %v, want all four", resp.GetAttempts())
	}
	for _, attempt := range resp.GetAttempts() {
		if attempt.GetCapacityWait() != nil {
			t.Errorf("%s: capacity_wait = %v, want none", attempt.GetTaskId(), attempt.GetCapacityWait())
		}
	}
	if fleet.observeOrkers != 0 || len(fleet.describes) != 0 {
		t.Fatalf("inventory reads = %d, describes = %v; want none", fleet.observeOrkers, fleet.describes)
	}
}

// TestInspectRun_AnUnreadableInventoryLeavesTheAttemptWaitingForAnOrker: the
// capacity wait is a refinement of "waiting for an Orker", which stays true
// whatever the inventory says, so a failed read answers the attempts as they
// are rather than failing the live view.
func TestInspectRun_AnUnreadableInventoryLeavesTheAttemptWaitingForAnOrker(t *testing.T) {
	run := inspectRun("ork-capacity-3", orkstackv1.Status_STATUS_RUNNING, "build")
	routeTasksTo(run, "build", "build")
	fleet := capacityFleet()
	fleet.observeEr = errors.New("frontend unavailable")
	f := newCapacityInspectFixture(t, run, []*orkstackv1.LiveAttempt{waitingAttempt("build")}, fleet)

	resp, err := f.inspect(t, &orkstackv1.InspectRunRequest{RunId: run.GetId()})
	if err != nil {
		t.Fatalf("InspectRun over an unreadable inventory: %v, want the live attempts", err)
	}
	if len(resp.GetAttempts()) != 1 || resp.GetAttempts()[0].GetCapacityWait() != nil {
		t.Fatalf("attempts = %v, want the one SCHEDULED attempt with no capacity_wait", resp.GetAttempts())
	}
	if fleet.observeOrkers != 1 {
		t.Fatalf("inventory reads = %d, want the one attempted read", fleet.observeOrkers)
	}
}

// TestInspectRun_AnOrkerThatDidNotAnswerKeepsEveryPoolFromBeingCalledFull:
// the Orker that did not answer could serve the waiting task's pool, with
// room, so "every orker of the pool is full" is not known and is not said.
func TestInspectRun_AnOrkerThatDidNotAnswerKeepsEveryPoolFromBeingCalledFull(t *testing.T) {
	run := inspectRun("ork-capacity-4", orkstackv1.Status_STATUS_RUNNING, "build")
	routeTasksTo(run, "build", "build")
	fleet := capacityFleet()
	fleet.presence = append(fleet.presence, engine.OrkerPresence{Identity: "orker-b2", LastSeen: fleetSeen})
	fleet.failures = map[string]error{"orker-b2": errors.New("describe orker: context deadline exceeded")}
	f := newCapacityInspectFixture(t, run, []*orkstackv1.LiveAttempt{waitingAttempt("build")}, fleet)

	resp, err := f.inspect(t, &orkstackv1.InspectRunRequest{RunId: run.GetId()})
	if err != nil {
		t.Fatalf("InspectRun: %v", err)
	}
	if got := resp.GetAttempts()[0].GetCapacityWait(); got != nil {
		t.Fatalf("capacity_wait = %v with an Orker unanswered, want none", got)
	}
	if len(fleet.describes) != 3 {
		t.Fatalf("describes = %v, want every live Orker asked", fleet.describes)
	}
}

// hungFleet is a stubFleet one of whose Orkers never answers until it is
// given up on, and which records the deadline it was given.
type hungFleet struct {
	*stubFleet
	hung     string
	mu       sync.Mutex
	deadline time.Time
}

func (f *hungFleet) DescribeOrkerByID(ctx context.Context, id string) (*orkstackv1.DescribeOrkerResult, error) {
	if id != f.hung {
		return f.stubFleet.DescribeOrkerByID(ctx, id)
	}
	deadline, _ := ctx.Deadline()
	f.mu.Lock()
	f.deadline = deadline
	f.mu.Unlock()
	<-ctx.Done()
	return nil, ctx.Err()
}

// TestInspectRun_AHungOrkerStillAnswersWithinTheReadBound: an Orker that
// never answers is given up on at the read's own bound, well inside the live
// view's poll, and the attempt keeps today's caption.
func TestInspectRun_AHungOrkerStillAnswersWithinTheReadBound(t *testing.T) {
	run := inspectRun("ork-capacity-5", orkstackv1.Status_STATUS_RUNNING, "build")
	routeTasksTo(run, "build", "build")
	stub := capacityFleet()
	stub.presence = append(stub.presence, engine.OrkerPresence{Identity: "orker-hung", LastSeen: fleetSeen})
	fleet := &hungFleet{stubFleet: stub, hung: "orker-hung"}
	reader := &fakeReader{runs: map[string]*orkstackv1.Run{run.GetId(): run}}
	liveReader := &liveFakeReader{fakeReader: reader, live: engine.LiveRun{Attempts: []*orkstackv1.LiveAttempt{waitingAttempt("build")}}}
	f := newReaderFixtureWith(t, reader, func(cfg *api.Config) {
		cfg.Reader = liveReader
		cfg.Now = func() time.Time { return inspectNow }
		cfg.Fleet = fleet
	})
	asked := time.Now()
	resp, err := (&inspectFixture{readerFixture: f, live: liveReader}).inspect(t,
		&orkstackv1.InspectRunRequest{RunId: run.GetId()})
	if err != nil {
		t.Fatalf("InspectRun with a hung Orker: %v", err)
	}
	if got := resp.GetAttempts()[0].GetCapacityWait(); got != nil {
		t.Fatalf("capacity_wait = %v with an Orker that never answered, want today's caption", got)
	}
	fleet.mu.Lock()
	deadline := fleet.deadline
	fleet.mu.Unlock()
	if deadline.IsZero() || deadline.Sub(asked) > 3*time.Second {
		t.Fatalf("the hung Orker was asked with deadline %v, %v after the call; want the 2 s read bound",
			deadline, deadline.Sub(asked))
	}
}

// TestInspectRun_AFailedInventoryReadIsWarnedAtMostOncePerInterval: every
// open run page polls, so a failed read is warned once per interval per
// replica rather than once per poll per viewer.
func TestInspectRun_AFailedInventoryReadIsWarnedAtMostOncePerInterval(t *testing.T) {
	run := inspectRun("ork-capacity-6", orkstackv1.Status_STATUS_RUNNING, "build")
	routeTasksTo(run, "build", "build")
	fleet := capacityFleet()
	fleet.observeEr = errors.New("frontend unavailable")
	var logs syncBuffer
	now := inspectNow
	var clock sync.Mutex
	reader := &fakeReader{runs: map[string]*orkstackv1.Run{run.GetId(): run}}
	liveReader := &liveFakeReader{fakeReader: reader, live: engine.LiveRun{Attempts: []*orkstackv1.LiveAttempt{waitingAttempt("build")}}}
	fixture := newReaderFixtureWith(t, reader, func(cfg *api.Config) {
		cfg.Reader = liveReader
		cfg.Now = func() time.Time { clock.Lock(); defer clock.Unlock(); return now }
		cfg.Fleet = fleet
		cfg.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	})
	f := &inspectFixture{readerFixture: fixture, live: liveReader}
	warned := func() int {
		return strings.Count(logs.String(), "the Orker inventory could not be read")
	}
	for i := 0; i < 3; i++ {
		if _, err := f.inspect(t, &orkstackv1.InspectRunRequest{RunId: run.GetId()}); err != nil {
			t.Fatalf("InspectRun: %v", err)
		}
	}
	if got := warned(); got != 1 {
		t.Fatalf("three failed reads warned %d times, want 1", got)
	}
	if fleet.observeOrkers != 3 {
		t.Fatalf("presence reads = %d, want 3: a failure is never kept", fleet.observeOrkers)
	}
	clock.Lock()
	now = now.Add(time.Minute)
	clock.Unlock()
	if _, err := f.inspect(t, &orkstackv1.InspectRunRequest{RunId: run.GetId()}); err != nil {
		t.Fatalf("InspectRun: %v", err)
	}
	if got := warned(); got != 2 {
		t.Fatalf("a failure a minute later warned %d times in all, want 2", got)
	}
}
