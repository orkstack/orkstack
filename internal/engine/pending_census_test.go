package engine_test

import (
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

var censusNow = time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)

// censusOrker is one live Orker of pool and build, answering on its channel.
func censusOrker(id, pool, build string, edit func(*orkstackv1.DescribeOrkerResult)) engine.FleetOrker {
	d := &orkstackv1.DescribeOrkerResult{OrkerId: id, Pool: pool, BuildId: build, MaxConcurrent: 4}
	if edit != nil {
		edit(d)
	}
	return engine.FleetOrker{Presence: engine.OrkerPresence{Identity: id, LastSeen: censusNow}, Described: d}
}

func asStopping(d *orkstackv1.DescribeOrkerResult) {
	d.Cordoned = true
	d.Stopping = &orkstackv1.OrkerStopping{}
}

func asCordoned(d *orkstackv1.DescribeOrkerResult) {
	d.Cordoned = true
	d.CordonReason = "maintenance"
}

func asPinHost(d *orkstackv1.DescribeOrkerResult) { d.MaxConcurrentPins = 2 }

// waitingRow is a task the run dispatched a while ago that no Orker started.
func waitingRow(id, pool string) *orkstackv1.TaskRun {
	return &orkstackv1.TaskRun{TaskId: id, Pool: pool, Status: orkstackv1.Status_STATUS_RUNNING,
		ScheduledAt: timestamppb.New(censusNow.Add(-time.Minute))}
}

func censusRun(rows ...*orkstackv1.TaskRun) *orkstackv1.Run {
	return &orkstackv1.Run{Id: "ork-census", Status: orkstackv1.Status_STATUS_RUNNING, TaskRuns: rows}
}

func reasonOf(run *orkstackv1.Run, id string) string {
	for _, row := range run.GetTaskRuns() {
		if row.GetTaskId() == id {
			return row.GetPendingReason()
		}
	}
	return "<no row " + id + ">"
}

// waits is a dispatch whose named task activities wait for an Orker.
func waits(build string, keys ...engine.WaitingAttempt) engine.RunDispatch {
	d := engine.RunDispatch{Build: build, Scheduled: len(keys) > 0, Waiting: map[engine.WaitingAttempt]bool{}}
	for _, k := range keys {
		d.Waiting[k] = true
	}
	return d
}

func own(task string) engine.WaitingAttempt { return engine.WaitingAttempt{TaskID: task, Item: -1} }

// TestReader_PendingReasonPrecedenceAndFreshness is E92 §4.9's census half:
// a task whose activity waits for an Orker, on a pool where no Orker takes
// work, says so with the pool's stopping, paused and cordoned Orkers; a
// pinned task of the pin being opened says it waits for a pin host. A reason
// a row already carries (its own wait to run again) stands. Nothing is
// claimed for a task whose container runs (RUNNING with no started_at, which
// the snapshot fills only when an attempt ends), when a live Orker did not
// answer, when an Orker takes work there, for a pinned task of another pin,
// or for a pin in a paused run.
func TestReader_PendingReasonPrecedenceAndFreshness(t *testing.T) {
	running := waitingRow("running", "build") // its container runs: not in Waiting
	ownReason := waitingRow("own", "build")
	ownReason.Status = orkstackv1.Status_STATUS_PENDING
	ownReason.PendingReason = "waiting until 10:42:00 UTC to run again (its Orker was lost)"
	await := waitingRow("approve", "build")
	await.Await = &orkstackv1.TaskAwait{}
	member := &orkstackv1.TaskRun{TaskId: "member", Pool: "build", Pin: "p", Status: orkstackv1.Status_STATUS_PENDING}
	otherPin := &orkstackv1.TaskRun{TaskId: "other", Pool: "build", Pin: "q", Status: orkstackv1.Status_STATUS_PENDING}
	opened := &orkstackv1.TaskRun{TaskId: "opened", Pool: "build", Pin: "q", Status: orkstackv1.Status_STATUS_RUNNING}

	stoppingPausedCordoned := []engine.FleetOrker{
		censusOrker("o1", "build", "", asStopping),
		censusOrker("o2", "build", "", asCordoned),
		censusOrker("o3", "build", "", func(d *orkstackv1.DescribeOrkerResult) {
			d.Cordoned = true
			d.StoreOutage = &orkstackv1.StoreOutage{}
		}),
		censusOrker("g1", "gpu", "", nil),
	}
	stoppingPinHost := []engine.FleetOrker{
		censusOrker("o1", "build", "", nil),
		censusOrker("o2", "build", "", func(d *orkstackv1.DescribeOrkerResult) { asPinHost(d); asStopping(d) }),
	}
	opening := engine.RunDispatch{OpeningPins: true}
	cases := []struct {
		name     string
		orkers   []engine.FleetOrker
		dispatch engine.RunDispatch
		paused   bool
		rows     []*orkstackv1.TaskRun
		want     map[string]string // task id -> reason; "" means none
	}{
		{
			name:     "no Orker of the pool takes work",
			orkers:   stoppingPausedCordoned,
			dispatch: waits("", own("fetch"), own("own"), own("approve")),
			rows:     []*orkstackv1.TaskRun{waitingRow("fetch", "build"), running, ownReason, await},
			want: map[string]string{
				"fetch":   "waiting for an Orker on pool build; none takes work (1 stopping, 1 paused, 1 cordoned)",
				"running": "",
				"own":     ownReason.GetPendingReason(),
				"approve": "",
			},
		},
		{
			name:     "an Orker of the pool takes work",
			orkers:   append(append([]engine.FleetOrker{}, stoppingPausedCordoned...), censusOrker("o4", "build", "", nil)),
			dispatch: waits("", own("fetch")),
			rows:     []*orkstackv1.TaskRun{waitingRow("fetch", "build")},
			want:     map[string]string{"fetch": ""},
		},
		{
			name:     "a pool no Orker serves at all",
			orkers:   stoppingPausedCordoned,
			dispatch: waits("", own("render")),
			rows:     []*orkstackv1.TaskRun{waitingRow("render", "render")},
			want:     map[string]string{"render": "waiting for an Orker on pool render; none serves it"},
		},
		{
			name: "a live Orker that did not answer claims nothing",
			orkers: append(append([]engine.FleetOrker{}, stoppingPausedCordoned...), engine.FleetOrker{
				Presence: engine.OrkerPresence{Identity: "silent", LastSeen: censusNow}, Err: errors.New("timeout")}),
			dispatch: waits("", own("fetch")),
			rows:     []*orkstackv1.TaskRun{waitingRow("fetch", "build")},
			want:     map[string]string{"fetch": ""},
		},
		{
			name:     "a pinned task of the opening pin waits for a pin host",
			orkers:   stoppingPinHost,
			dispatch: opening,
			rows:     []*orkstackv1.TaskRun{member, otherPin, opened},
			want:     map[string]string{"member": "waiting for a pin host on pool build; none takes work (1 stopping)", "other": ""},
		},
		{
			name:     "two pins that could be opening: nothing is said",
			orkers:   stoppingPinHost,
			dispatch: opening,
			rows:     []*orkstackv1.TaskRun{member, otherPin},
			want:     map[string]string{"member": "", "other": ""},
		},
		{
			name:     "a pool with Orkers but no pin host",
			orkers:   []engine.FleetOrker{censusOrker("o1", "build", "", nil)},
			dispatch: opening,
			rows:     []*orkstackv1.TaskRun{member},
			want:     map[string]string{"member": "waiting for a pin host on pool build; none serves it"},
		},
		{
			name:     "a paused run's pin says nothing",
			orkers:   stoppingPinHost,
			dispatch: opening,
			paused:   true,
			rows:     []*orkstackv1.TaskRun{member},
			want:     map[string]string{"member": ""},
		},
		{
			name:   "a pinned task whose pin is not opening says nothing",
			orkers: stoppingPinHost,
			rows:   []*orkstackv1.TaskRun{member},
			want:   map[string]string{"member": ""},
		},
		{
			name:     "a pin host taking work says nothing",
			orkers:   []engine.FleetOrker{censusOrker("o1", "build", "", asPinHost)},
			dispatch: opening,
			rows:     []*orkstackv1.TaskRun{member},
			want:     map[string]string{"member": ""},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Each case reads its own copy: the function writes the rows.
			rows := make([]*orkstackv1.TaskRun, len(tc.rows))
			for i, row := range tc.rows {
				rows[i] = proto.Clone(row).(*orkstackv1.TaskRun)
			}
			run := censusRun(rows...)
			if tc.paused {
				run.Pause = &orkstackv1.RunPause{}
			}
			engine.ApplyCensusPendingReasons(run, tc.orkers, tc.dispatch, censusNow)
			for id, want := range tc.want {
				if got := reasonOf(run, id); got != want {
					t.Errorf("%s: pending_reason = %q, want %q", id, got, want)
				}
			}
		})
	}

	t.Run("a fan-out's items each say why, by their own pool", func(t *testing.T) {
		item0, item1 := int32(0), int32(1)
		first, second := waitingRow("spread", "build"), waitingRow("spread", "gpu")
		first.Item, second.Item = &item0, &item1
		run := censusRun(&orkstackv1.TaskRun{TaskId: "spread", Pool: "build", Status: orkstackv1.Status_STATUS_RUNNING,
			Each: &orkstackv1.TaskEach{}, Items: []*orkstackv1.TaskRun{first, second}})
		engine.ApplyCensusPendingReasons(run, stoppingPausedCordoned,
			waits("", engine.WaitingAttempt{TaskID: "spread", Item: 0}, engine.WaitingAttempt{TaskID: "spread", Item: 1}), censusNow)
		items := run.GetTaskRuns()[0].GetItems()
		if got := items[0].GetPendingReason(); got != "waiting for an Orker on pool build; none takes work (1 stopping, 1 paused, 1 cordoned)" {
			t.Errorf("item 0 = %q", got)
		}
		if got := items[1].GetPendingReason(); got != "" {
			t.Errorf("item 1, on a pool an Orker serves, = %q; want none", got)
		}
	})
	t.Run("a finished run says nothing", func(t *testing.T) {
		run := censusRun(waitingRow("fetch", "build"))
		run.Status = orkstackv1.Status_STATUS_FAILED
		engine.ApplyCensusPendingReasons(run, stoppingPausedCordoned, waits("", own("fetch")), censusNow)
		if got := reasonOf(run, "fetch"); got != "" {
			t.Errorf("pending_reason = %q on a finished run", got)
		}
	})
}

// TestWaitsForAnOrker: only a SCHEDULED activity outside a retry back-off,
// scheduled at least DispatchWaitAfter ago, waits for an Orker to take it.
func TestWaitsForAnOrker(t *testing.T) {
	scheduled := func(ago time.Duration) *orkstackv1.LiveAttempt {
		return &orkstackv1.LiveAttempt{TaskId: "fetch", State: orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_SCHEDULED,
			AttemptScheduledAt: timestamppb.New(censusNow.Add(-ago))}
	}
	started := scheduled(time.Minute)
	started.State = orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_STARTED
	backoff := scheduled(time.Minute)
	backoff.NextAttemptAt = timestamppb.New(censusNow.Add(time.Minute))
	for _, tc := range []struct {
		name    string
		attempt *orkstackv1.LiveAttempt
		want    bool
	}{
		{"scheduled a minute ago", scheduled(time.Minute), true},
		{"scheduled a moment ago", scheduled(time.Second), false},
		{"started", started, false},
		{"in a retry's back-off", backoff, false},
	} {
		if got := engine.WaitsForAnOrker(tc.attempt, censusNow); got != tc.want {
			t.Errorf("%s: WaitsForAnOrker = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestReader_PendingReasonNamesTheRunsRelease: on a versioned fleet the run's
// tasks go only to Orkers of its own release, so the census counts only
// those, and the reason names the release: an Orker of another release taking
// work on the pool does not answer for it.
func TestReader_PendingReasonNamesTheRunsRelease(t *testing.T) {
	orkers := []engine.FleetOrker{
		censusOrker("old-1", "build", "b1", asStopping),
		censusOrker("new-1", "build", "b2", nil),
	}
	run := censusRun(waitingRow("fetch", "build"))
	engine.ApplyCensusPendingReasons(run, orkers, waits("b1", own("fetch")), censusNow)
	want := "waiting for an Orker of release b1 on pool build; none takes work (1 stopping)"
	if got := reasonOf(run, "fetch"); got != want {
		t.Fatalf("pending_reason = %q, want %q", got, want)
	}

	// The release that does take work there leaves the row alone.
	run = censusRun(waitingRow("fetch", "build"))
	engine.ApplyCensusPendingReasons(run, orkers, waits("b2", own("fetch")), censusNow)
	if got := reasonOf(run, "fetch"); got != "" {
		t.Fatalf("pending_reason = %q for a run whose release takes work on the pool", got)
	}

	// A release none of whose Orkers serves the pool any more.
	run = censusRun(waitingRow("fetch", "build"))
	engine.ApplyCensusPendingReasons(run, orkers, waits("b0", own("fetch")), censusNow)
	if got, want := reasonOf(run, "fetch"), "waiting for an Orker of release b0 on pool build; none serves it"; got != want {
		t.Fatalf("pending_reason = %q, want %q", got, want)
	}
}
