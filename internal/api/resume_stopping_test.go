package api_test

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// openAttemptHistory is failedHistory with task b's last attempt still open
// at the close, and the time it closed.
func openAttemptHistory(t *testing.T) ([]*historypb.HistoryEvent, time.Time) {
	b := &resumeHistory{t: t, events: failedHistory(t, "a")}
	b.events = b.events[:len(b.events)-1]
	b.schedule(task.ActivityExecuteTask, &orkstackv1.ExecuteTaskInput{TaskId: "b", CancellationGracePeriodSeconds: 30})
	id := b.add(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, nil)
	return b.events, b.events[id-1].GetEventTime().AsTime()
}

// TestResumeRun_ContainersStillStopping: a resume waits until no container of
// the run can still be running, says when that is, and goes on at that
// instant.
func TestResumeRun_ContainersStillStopping(t *testing.T) {
	events, closed := openAttemptHistory(t)
	bound := closed.Add(20*time.Second + 30*time.Second + 30*time.Second)
	for name, tc := range map[string]struct {
		now   time.Time
		retry int64 // 0: not refused
	}{
		"at the close":          {closed, 80},
		"just before the bound": {bound.Add(-500 * time.Millisecond), 1},
		"at the bound":          {bound, 0},
	} {
		t.Run(name, func(t *testing.T) {
			f := newResumeFixture(t, func(f *resumeFixture) { f.cluster.histories["exec-1"] = events })
			f.clock.set(tc.now)
			_, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
			if tc.retry == 0 {
				if connect.CodeOf(err) == connect.CodeFailedPrecondition {
					t.Fatalf("refused at the bound: %v", err)
				}
				return
			}
			r := refusalOf(t, err)
			if r.GetReason() != "STILL_STOPPING" || r.GetCause() != "containers_stopping" ||
				r.GetClass() != orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_WAIT || r.GetRetryAfterSeconds() != tc.retry {
				t.Fatalf("refusal = %v, want STILL_STOPPING containers_stopping in %d s", r, tc.retry)
			}
		})
	}
}

// childHistory is failedHistory with child runs started, those in ended seen
// to end before the close.
func childHistory(t *testing.T, children []string, ended ...string) ([]*historypb.HistoryEvent, time.Time) {
	b := &resumeHistory{t: t, events: failedHistory(t, "a", "b")}
	b.events = b.events[:len(b.events)-1]
	for _, id := range children {
		initiated := b.add(enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED, func(ev *historypb.HistoryEvent) {
			ev.Attributes = &historypb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{
				StartChildWorkflowExecutionInitiatedEventAttributes: &historypb.StartChildWorkflowExecutionInitiatedEventAttributes{WorkflowId: id}}
		})
		if slices.Contains(ended, id) {
			b.add(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED, func(ev *historypb.HistoryEvent) {
				ev.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionFailedEventAttributes{
					ChildWorkflowExecutionFailedEventAttributes: &historypb.ChildWorkflowExecutionFailedEventAttributes{InitiatedEventId: initiated}}
			})
		}
	}
	id := b.add(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, nil)
	return b.events, b.events[id-1].GetEventTime().AsTime()
}

// TestResumeRun_ChildRunsStillOpen: a run is not resumed while a run below
// it is open: a child it did not see end is asked about itself, and the rest
// are looked for among the runs whose root it is, once the cluster's index
// has had time to show them.
func TestResumeRun_ChildRunsStillOpen(t *testing.T) {
	live := func(id string) *orkstackv1.Run {
		return &orkstackv1.Run{Id: id, Status: orkstackv1.Status_STATUS_RUNNING, Origin: &orkstackv1.RunOrigin{
			Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN, Id: "ork-child", RootRunId: "ork-r"}}
	}
	open := engine.ResumeExecution{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}
	// Open two days before its parent closed: past a day's wait, the advice
	// leads with ending it.
	long := engine.ResumeExecution{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, StartTime: resumeEpoch.Add(-48 * time.Hour)}
	closedChild := engine.ResumeExecution{Status: enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED}
	for name, tc := range map[string]struct {
		ended  []string
		child  *engine.ResumeExecution
		runs   []*orkstackv1.Run
		early  bool // before the index can be trusted
		list   error
		cause  string // "": not refused
		naming string
		says   string
	}{
		"every child ended":               {ended: []string{"ork-child"}},
		"a child it did not see end":      {child: &open, cause: "child_runs_open", naming: "ork-child", says: "wait for them, or end them with orkctl run cancel,"},
		"a child open for days":           {child: &long, cause: "child_runs_open", naming: "ork-child", says: "have been open since"},
		"a child that has ended since":    {child: &closedChild},
		"a child whose history is gone":   {},
		"an orphan further down":          {ended: []string{"ork-child"}, runs: []*orkstackv1.Run{live("ork-orphan")}, cause: "child_runs_open", naming: "ork-orphan"},
		"another run's child":             {ended: []string{"ork-child"}, runs: []*orkstackv1.Run{{Id: "ork-x", Status: orkstackv1.Status_STATUS_RUNNING}}},
		"before the index can be trusted": {ended: []string{"ork-child"}, early: true, cause: "children_unconfirmed"},
		"an index that does not answer":   {ended: []string{"ork-child"}, list: errors.New("unreachable"), cause: "children_unconfirmed"},
	} {
		t.Run(name, func(t *testing.T) {
			events, closed := childHistory(t, []string{"ork-child"}, tc.ended...)
			f := newResumeFixture(t, func(f *resumeFixture) {
				f.cluster.histories["exec-1"] = events
				if tc.early {
					f.settings.VisibilityLagFloor = time.Hour
				}
				e := f.cluster.executions[""]
				e.CloseTime = closed
				f.cluster.executions[""] = e
				if tc.child != nil {
					f.cluster.children = map[string]engine.ResumeExecution{"ork-child": *tc.child}
				}
			})
			for _, run := range tc.runs {
				f.reader.runs[run.GetId()] = run
			}
			f.reader.listErr = tc.list
			f.clock.set(closed.Add(time.Minute))
			_, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
			if tc.cause == "" {
				if connect.CodeOf(err) == connect.CodeFailedPrecondition {
					t.Fatalf("refused: %v", err)
				}
				return
			}
			r := refusalOf(t, err)
			if r.GetReason() != "STILL_STOPPING" || r.GetCause() != tc.cause || r.GetRetryAfterSeconds() <= 0 ||
				!strings.Contains(r.GetMessage(), tc.naming) || !strings.Contains(r.GetMessage(), tc.says) {
				t.Fatalf("refusal = %v, want STILL_STOPPING %s naming %q", r, tc.cause, tc.naming)
			}
		})
	}
}

// TestResumeRun_NoChildRunsAsksNothing: a run that started no child run costs
// no search.
func TestResumeRun_NoChildRunsAsksNothing(t *testing.T) {
	f := newResumeFixture(t, nil)
	_, _ = f.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
	if len(f.reader.listQueries) != 0 {
		t.Fatalf("asked %v", f.reader.listQueries)
	}
}

// TestResumeRun_PinLeasesHeld: a pin that runs again waits for its run's own
// old pin only while every pin host of its pool is full and one is held by
// it, and is asked to try again shortly: a host frees a closed run's slot at
// its next answered heartbeat, long before the lease ends.
func TestResumeRun_PinLeasesHeld(t *testing.T) {
	now := resumeEpoch.Add(24 * time.Hour)
	lease := func(run string, left time.Duration) *orkstackv1.OpenPin {
		return &orkstackv1.OpenPin{RunId: run, Pin: "p", ExpiresAt: timestamppb.New(now.Add(left))}
	}
	host := func(pins ...*orkstackv1.OpenPin) *orkstackv1.DescribeOrkerResult {
		return &orkstackv1.DescribeOrkerResult{Pool: "main", AttemptOrdinalsSupported: true, MaxConcurrentPins: 1, OpenPins: int32(len(pins)), Pins: pins}
	}
	cordoned := host()
	cordoned.Cordoned = true
	for name, tc := range map[string]struct {
		hosts []*orkstackv1.DescribeOrkerResult
		retry int64
	}{
		"its own lease holds the only host":  {[]*orkstackv1.DescribeOrkerResult{host(lease("ork-r", 90*time.Second))}, 15},
		"its lease ends just past the retry": {[]*orkstackv1.DescribeOrkerResult{host(lease("ork-r", 16*time.Second))}, 15},
		"its lease ends just before it":      {[]*orkstackv1.DescribeOrkerResult{host(lease("ork-r", 14*time.Second))}, 14},
		"the earliest of its leases":         {[]*orkstackv1.DescribeOrkerResult{host(lease("ork-r", 90*time.Second)), host(lease("ork-r", 5*time.Second))}, 5},
		"others hold every host":             {[]*orkstackv1.DescribeOrkerResult{host(lease("ork-z", time.Minute))}, 0},
		"another host is free":               {[]*orkstackv1.DescribeOrkerResult{host(lease("ork-r", time.Minute)), host()}, 0},
		"another host is free for two tasks": {[]*orkstackv1.DescribeOrkerResult{host(lease("ork-r", time.Minute)), host()}, 0},
		"the free host is cordoned":          {[]*orkstackv1.DescribeOrkerResult{host(lease("ork-r", time.Minute)), cordoned}, 15},
		"its lease has expired":              {[]*orkstackv1.DescribeOrkerResult{host(lease("ork-r", -time.Minute))}, 0},
	} {
		t.Run(name, func(t *testing.T) {
			f := newResumeFixture(t, func(f *resumeFixture) {
				f.def.Tasks[1].Pin = "p"
				if name == "another host is free for two tasks" {
					f.def.Tasks[0].Pin = "p"
				}
				f.fleet.described = map[string]*orkstackv1.DescribeOrkerResult{}
				for i, h := range tc.hosts {
					id := fmt.Sprintf("orker-%d", i)
					h.OrkerId = id
					f.fleet.presence = append(f.fleet.presence, engine.OrkerPresence{Identity: id, LastSeen: now})
					f.fleet.described[id] = h
				}
			})
			got, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
			if tc.retry == 0 {
				if err != nil && refusalOf(t, err).GetCause() == "pin_leases_held" {
					t.Fatalf("refused: %v", err)
				}
				// Opening on a free host, it is told its old one may still
				// count the slot, once for the pin however many of its tasks
				// run again.
				warned := 0
				for _, w := range got.GetWarnings() {
					if w.GetKind() == orkstackv1.ResumeWarningKind_RESUME_WARNING_KIND_PIN_LEASE_HELD && w.GetPin() == "p" && w.GetUntil().IsValid() {
						warned++
					}
				}
				want := 0
				if strings.HasPrefix(name, "another host is free") {
					want = 1
				}
				if err == nil && warned != want {
					t.Fatalf("warnings = %v; want %d held pin named", got.GetWarnings(), want)
				}
				return
			}
			if r := refusalOf(t, err); r.GetReason() != "STILL_STOPPING" || r.GetCause() != "pin_leases_held" || r.GetRetryAfterSeconds() != tc.retry ||
				!strings.Contains(r.GetMessage(), "frees it shortly") || strings.Contains(r.GetMessage(), "until") {
				t.Fatalf("refusal = %v, want pin_leases_held in %d s", r, tc.retry)
			}
		})
	}
}
