package engine

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// drainWorld runs fn in workflow code over a run that awaits gate and other,
// with queued messages naming the awaits in queue, and answers each await's
// read with answers. It says which were read.
func drainWorld(t *testing.T, queue []string, answer func(id string) (ResolutionRead, error), edit func(*runState), fn func(workflow.Context, *runState)) []string {
	t.Helper()
	return drainCounted(t, nil, queue, answer, edit, fn)
}

// drainCounted is drainWorld with the run's counts kept in counts.
func drainCounted(t *testing.T, counts *countedRecords, queue []string, answer func(id string) (ResolutionRead, error), edit func(*runState), fn func(workflow.Context, *runState)) []string {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	if counts != nil {
		suite.SetMetricsHandler(counts)
	}
	env := suite.NewTestWorkflowEnvironment()
	var read []string
	env.RegisterActivityWithOptions(func(ctx context.Context, req ResolutionReadRequest) (ResolutionRead, error) {
		if activity.GetInfo(ctx).Attempt == 1 {
			read = append(read, req.TaskID)
		}
		return answer(req.TaskID)
	}, activity.RegisterOptions{Name: ActivityReadResolution})
	budget := DefaultRecordBudget
	env.RegisterDelayedCallback(func() {
		for _, id := range queue {
			env.SignalWorkflow(ResolveAwaitSignal, &orkstackv1.AwaitSignal{TaskId: id})
		}
	}, time.Second)
	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		s := &runState{runID: "ork-1", wf: &orkstackv1.Orkflow{}, order: []string{"gate", "other"}, runCtx: ctx,
			plan: RunPlan{Identity: &IdentityPlan{ValueRecords: 1, RecordBudget: &budget, ResolutionsPerPass: 4, NonesPerAwait: 3}},
			taskRuns: map[string]*orkstackv1.TaskRun{"gate": {Status: orkstackv1.Status_STATUS_AWAITING},
				"other": {Status: orkstackv1.Status_STATUS_AWAITING}},
			awaits:      map[string]*awaitState{"gate": {open: true, deadlineAt: workflow.Now(ctx).Add(time.Hour)}, "other": {open: true, deadlineAt: workflow.Now(ctx).Add(time.Hour)}},
			resolutions: workflow.GetSignalChannel(ctx, ResolveAwaitSignal)}
		if edit != nil {
			edit(s)
		}
		_ = workflow.Sleep(ctx, 2*time.Second)
		fn(ctx, s)
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	return read
}

func notAnswered(string) (ResolutionRead, error) {
	return ResolutionRead{}, temporal.NewApplicationError("denied: 403", RecordStoreUnansweredType)
}

func newerFormat(string) (ResolutionRead, error) {
	return ResolutionRead{Outcome: ResolutionUnsupported}, nil
}

// TestSwitchRecorded: a switch reads what is queued, bounded; a read that is
// not answered changes nothing, and what is queued behind it is dropped, so
// the run carries on with its awaits open; what it leaves queued holds it.
func TestSwitchRecorded(t *testing.T) {
	for _, tc := range []struct {
		name    string
		queue   []string
		answer  func(string) (ResolutionRead, error)
		edit    func(*runState)
		changed bool
		read    string
		left    int
	}{
		{"nothing queued", nil, notAnswered, nil, false, "", 0},
		{"a read that is not answered", []string{"gate", "other", "gate", "other"}, notAnswered, nil, false, "gate", 0},
		{"while reads go unanswered", []string{"gate", "other"}, notAnswered, func(s *runState) { s.reads.unanswered = true }, false, "", 0},
		{"a newer format, which the store answered", []string{"gate", "other", "gate"}, newerFormat, nil, false, "gate other", 0},
		{"more than a pass takes", []string{"gate", "gate", "gate", "gate", "gate", "gate"}, newerFormat, nil, true, "gate", 2},
		{"an ending that is ready", []string{"gate"}, newerFormat, func(s *runState) {
			s.runTimeout = &runTimeout{deadline: time.Unix(1, 0)}
			s.failingPhase, s.runTimeout.hardEnded = true, true
		}, true, "", 1},
		{"no history for a read", []string{"gate"}, newerFormat, func(s *runState) {
			s.plan.History = &HistoryLimit{MaxEvents: 1, MaxBytes: 1, Reserve: DefaultHistoryReserve}
		}, false, "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			read := drainWorld(t, tc.queue, tc.answer, tc.edit, func(ctx workflow.Context, s *runState) {
				changed, id, err := s.switchRecorded(ctx, nil)
				if changed != tc.changed || id != "" || err != nil || s.resolutions.Len() != tc.left {
					t.Errorf("changed %v %q %v, %d left", changed, id, err, s.resolutions.Len())
				}
				for id, a := range s.awaits {
					if !a.open || s.taskRuns[id].GetStatus() != orkstackv1.Status_STATUS_AWAITING {
						t.Errorf("%s is closed", id)
					}
				}
			})
			if got := strings.Join(read, " "); got != tc.read {
				t.Fatalf("read %q, want %q", got, tc.read)
			}
		})
	}
}

// TestUndeferAwaits_InTheFailingPhase: a deadline an ending was to take is
// registered again in the next pass, but while the hard end is due and not yet
// taken, which its own timer, registered after the deadlines, then takes.
func TestUndeferAwaits_InTheFailingPhase(t *testing.T) {
	fired := func(ctx workflow.Context) workflow.Future {
		f := workflow.NewTimer(ctx, time.Millisecond)
		_ = f.Get(ctx, nil)
		return f
	}
	for _, tc := range []struct {
		name     string
		make     func(workflow.Context, *runState)
		deferred bool
		ready    bool // an ending is ready, or was taken
	}{
		{"an unpause won over the pause's expiry", func(workflow.Context, *runState) {}, false, false},
		{"a pause due and not taken", func(ctx workflow.Context, s *runState) {
			s.pauses, s.pause = workflow.GetSignalChannel(ctx, PauseSignal), &ownPause{timer: fired(ctx)}
		}, false, true},
		{"a pause's expiry taken", func(_ workflow.Context, s *runState) { s.pauseStop = &pauseStop{detail: "x"} }, false, true},
		{"the hard end due and not taken", func(ctx workflow.Context, s *runState) { s.runTimeout = &runTimeout{hardTimer: fired(ctx)} }, true, true},
		{"the hard end taken", func(ctx workflow.Context, s *runState) {
			s.runTimeout = &runTimeout{hardTimer: fired(ctx), hardTaken: true, hardEnded: true}
		}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			drainWorld(t, nil, notAnswered, nil, func(ctx workflow.Context, s *runState) {
				s.failingPhase = true
				tc.make(ctx, s)
				s.awaits["gate"].deferred = true
				s.beginPass()
				if got := s.awaits["gate"].deferred; got != tc.deferred {
					t.Errorf("deferred %v, want %v", got, tc.deferred)
				}
				if got := s.endingReady(ctx, nil); got != tc.ready {
					t.Errorf("an ending is ready %v, want %v", got, tc.ready)
				}
			})
		})
	}
}

// TestReadResolution_AnAnswerThatDoesNotOpen is not an answer: never that
// nobody answered, and never applied.
func TestReadResolution_AnAnswerThatDoesNotOpen(t *testing.T) {
	drainWorld(t, nil, func(string) (ResolutionRead, error) { return ResolutionRead{}, payloadUnreadable("x") }, nil, func(ctx workflow.Context, s *runState) {
		if got := s.readResolution(ctx, "gate"); got.unanswered != classInternal || got.err == nil || got.canceled || got.Outcome != "" {
			t.Errorf("read as %+v", got)
		}
	})
}

// countedRecords keeps what workflow code counted, by name and tags.
type countedRecords struct {
	client.MetricsHandler
	tags string
	seen map[string]int64
}

func (c *countedRecords) WithTags(tags map[string]string) client.MetricsHandler {
	out := c.tags
	for _, k := range slices.Sorted(maps.Keys(tags)) {
		if k == "kind" || k == "reason" || k == "why" {
			out += " " + k + "=" + tags[k]
		}
	}
	return &countedRecords{MetricsHandler: c.MetricsHandler, tags: out, seen: c.seen}
}

func (c *countedRecords) Counter(name string) client.MetricsCounter {
	return countedRecord{name + c.tags, c.seen}
}

type countedRecord struct {
	key  string
	seen map[string]int64
}

func (c countedRecord) Inc(n int64) { c.seen[c.key] += n }

// TestRecordCounters: one read that is not answered is one count, whatever is
// queued behind it, and what is dropped unread is counted by why.
func TestRecordCounters(t *testing.T) {
	for _, tc := range []struct {
		name   string
		queue  []string
		answer func(string) (ResolutionRead, error)
		want   map[string]int64
	}{
		{"denied", []string{"gate"}, notAnswered, map[string]int64{"ork.run.record_reads_unanswered kind=resolution reason=denied": 1}},
		{"denied, and more queued", []string{"gate", "other", "gate"}, notAnswered, map[string]int64{
			"ork.run.record_reads_unanswered kind=resolution reason=denied": 1, "ork.run.resolutions_coalesced why=stale": 2}},
		{"a newer format, and a repeat", []string{"gate", "gate"}, newerFormat, map[string]int64{
			"ork.run.record_reads_unanswered kind=resolution reason=unsupported": 1, "ork.run.resolutions_coalesced why=duplicate": 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			counts := &countedRecords{MetricsHandler: client.MetricsNopHandler, seen: map[string]int64{}}
			drainCounted(t, counts, tc.queue, tc.answer, nil, func(ctx workflow.Context, s *runState) {
				if _, _, err := s.switchRecorded(ctx, nil); err != nil {
					t.Error(err)
				}
			})
			got := map[string]int64{}
			for k, n := range counts.seen {
				if strings.HasPrefix(k, "ork.run.") {
					got[k] = n
				}
			}
			if !maps.Equal(got, tc.want) {
				t.Fatalf("counted %v, want %v", got, tc.want)
			}
		})
	}
}

// TestDrainRecordedAtPass reads nothing while the run's reads go unanswered.
func TestDrainRecordedAtPass(t *testing.T) {
	read := drainWorld(t, []string{"gate"}, newerFormat, func(s *runState) { s.reads.unanswered = true }, func(ctx workflow.Context, s *runState) {
		if _, err := s.drainRecordedAtPass(ctx, nil); err != nil || s.resolutions.Len() != 1 {
			t.Errorf("%v, %d left", err, s.resolutions.Len())
		}
	})
	if len(read) != 0 {
		t.Fatalf("read %v", read)
	}
}

// TestCanSwitchNow_AnOpenConfirmingSetHoldsIt: its members' second reads come
// before the run carries on.
func TestCanSwitchNow_AnOpenConfirmingSetHoldsIt(t *testing.T) {
	drainWorld(t, nil, notAnswered, nil, func(ctx workflow.Context, s *runState) {
		s.switchDue = true
		if !s.canSwitchNow(ctx, nil) {
			t.Error("a run with open awaits cannot switch")
		}
		s.awaits["gate"].confirming = true
		if s.canSwitchNow(ctx, nil) {
			t.Error("a run with a confirming set switches")
		}
	})
}

// TestFail_WithNoCause ends the run explicitly, never in a panic.
func TestFail_WithNoCause(t *testing.T) {
	failure := (&runState{}).runFailureFor("gate", fmt.Errorf("x"))
	if failure.GetCode() != "RUN_FAILED" {
		t.Fatalf("%v", failure)
	}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(func(context.Context, *orkstackv1.Run) (string, error) { return "", nil }, activity.RegisterOptions{Name: ActivityWriteSnapshot})
	var err error
	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		s := newRunState(ctx, "ork-1", &orkstackv1.Orkflow{}, nil, RunPlan{})
		_, err = s.fail(ctx, func() {}, map[string]workflow.Future{}, "gate", nil)
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "a failure with no cause") {
		t.Fatalf("%v, %v", err, env.GetWorkflowError())
	}
}

// TestAwaitReserve_CoversADeadlinesReads: a recorded run keeps back what an
// await's two reads and the set's timers can add, and no other run does.
func TestAwaitReserve_CoversADeadlinesReads(t *testing.T) {
	r := DefaultHistoryReserve
	budget := DefaultRecordBudget
	plain := &runState{}
	recorded := &runState{plan: RunPlan{Identity: &IdentityPlan{ValueRecords: 1, RecordBudget: &budget}}}
	if plain.awaitReserve(r) != r.PerAwait || recorded.awaitReserve(r) < r.PerAwait+2*budget.ReadEvents {
		t.Fatalf("%d and %d", plain.awaitReserve(r), recorded.awaitReserve(r))
	}
	child := &runState{plan: RunPlan{Identity: recorded.plan.Identity, Origin: &RunOrigin{Kind: RunOriginKindParentRun}}}
	if child.fixedReserve(r) != r.Fixed+budget.ReadEvents || recorded.fixedReserve(r) != r.Fixed {
		t.Fatalf("a child's %d, a root's %d", child.fixedReserve(r), recorded.fixedReserve(r))
	}
}
