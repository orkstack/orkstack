package engine_test

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// concurrencyRunFixture is one recorded run of E88 P3's run side
// (TestIntegration_RecordConcurrencyRunHistories): one command path each,
// recorded once and left to age (testdata/history/README.md).
type concurrencyRunFixtureCase struct {
	name    string
	closed  enumspb.EventType
	failure string // the failure type a FAILED close carries, "" for any
	// payload: the payload half re-executes it. A generation of a run that
	// continued is replayed by its command half only: its start is a
	// carry, not an input a test can re-execute from.
	payload bool
	// releases: it ends by signalling release_all to its gate.
	releases bool
}

func (f concurrencyRunFixtureCase) path() string {
	return filepath.Join("testdata", "history", "concurrency-run-"+f.name+"-v1.json")
}

var concurrencyRunFixtures = []concurrencyRunFixtureCase{
	{name: "granted", closed: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, payload: true, releases: true},
	{name: "queued", closed: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, payload: true, releases: true},
	{name: "requeued", closed: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, payload: true, releases: true},
	{name: "queue-canceled", closed: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED, payload: true, releases: true},
	{name: "queue-timeout", closed: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, failure: engine.RunQueueTimeoutCode,
		payload: true, releases: true},
	{name: "queue-timeout-rebuilding", closed: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		failure: engine.RunConcurrencyUnavailableCode, payload: true, releases: true},
	{name: "holds-across-continue-g0", closed: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW},
	{name: "holds-across-continue-g1", closed: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, releases: true},
	{name: "continue-gate-rebuild", closed: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, releases: true},
	{name: "emit-failure-releases", closed: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, payload: true, releases: true},
}

// requireConcurrencyRunFixture skips a fixture not yet recorded, naming the
// command that records it; ORK_REQUIRE_CONCURRENCY_FIXTURES makes its
// absence a failure.
func requireConcurrencyRunFixture(t *testing.T, f concurrencyRunFixtureCase) {
	t.Helper()
	if _, err := os.Stat(f.path()); err == nil {
		return
	} else if !errors.Is(err, fs.ErrNotExist) {
		t.Fatal(err)
	}
	const record = "ORK_RECORD_GOLDEN_HISTORY=1 ORK_REQUIRE_INFRA=1 go test -tags=integration -p 1 -count=1 " +
		"./internal/engine/ -run TestIntegration_RecordConcurrencyRunHistories"
	if os.Getenv("ORK_REQUIRE_CONCURRENCY_FIXTURES") != "" {
		t.Fatalf("%s is not recorded; record it with %s", f.path(), record)
	}
	t.Skipf("%s is not recorded yet; record it with %s", f.path(), record)
}

// concurrencyReplayID is the run id a fixture replays as: its start's, or,
// for a later generation, whose start names none and which reads its run id
// from its workflow id, the run its carried checkpoint is stored under
// (…/snapshots/{ns}/{run}/continuations/…), since restoring it checks that.
func concurrencyReplayID(t *testing.T, recorded *historypb.History) string {
	t.Helper()
	runID, _, _, _, carry := startArgs(t, recorded)
	if runID != "" {
		return runID
	}
	uri := carry.GetCheckpoint().GetUri()
	before, _, found := strings.Cut(uri, "/continuations/")
	runID = before[strings.LastIndex(before, "/")+1:]
	if !found || !strings.HasPrefix(runID, "ork-") {
		t.Fatalf("a later generation's carry names no checkpoint under a run: %q", uri)
	}
	return runID
}

func replayConcurrencyRun(recorded *historypb.History, runID string) error {
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	return replayer.ReplayWorkflowHistoryWithOptions(nil, recorded, worker.ReplayWorkflowHistoryOptions{
		OriginalExecution: workflow.Execution{ID: runID, RunID: "replay"},
	})
}

// TestRunWorkflow_ReplaysCommittedConcurrencyRunHistories is the command
// half of every run-side fixture: each closed as its path says, and replays
// against current code.
func TestRunWorkflow_ReplaysCommittedConcurrencyRunHistories(t *testing.T) {
	for _, f := range concurrencyRunFixtures {
		t.Run(f.name, func(t *testing.T) {
			requireConcurrencyRunFixture(t, f)
			recorded := loadHistoryFile(t, f.path())
			closed, failure := closingEvent(recorded)
			if closed != f.closed || (f.failure != "" && failure != f.failure) {
				t.Fatalf("%s closed %s %q, want %s %q", f.path(), closed, failure, f.closed, f.failure)
			}
			if err := replayConcurrencyRun(recorded, concurrencyReplayID(t, recorded)); err != nil {
				t.Fatalf("replaying %s against current code failed: %v\n\nA run with a concurrency limit in "+
					"flight when this build was deployed would fail its next workflow task. Restore the run "+
					"side's command stream, or re-record and say why.", f.path(), err)
			}
		})
	}
}

// TestConcurrencyRunHistories_CatchAMovedRelease is the negative test for
// every fixture that ends by releasing its slot: its release signal's name
// is rewritten, and the replayer must refuse it, so the fixture is known to
// pin the release in the command stream.
func TestConcurrencyRunHistories_CatchAMovedRelease(t *testing.T) {
	for _, f := range concurrencyRunFixtures {
		if !f.releases {
			continue
		}
		t.Run(f.name, func(t *testing.T) {
			requireConcurrencyRunFixture(t, f)
			recorded := loadHistoryFile(t, f.path())
			moved := false
			for _, event := range recorded.GetEvents() {
				if attrs := event.GetSignalExternalWorkflowExecutionInitiatedEventAttributes(); attrs != nil &&
					attrs.GetSignalName() == engine.GateSignalReleaseAll {
					attrs.SignalName = "moved-" + attrs.GetSignalName()
					moved = true
					break
				}
			}
			if !moved {
				t.Fatalf("%s signals no release: it does not record the run's release", f.path())
			}
			if err := replayConcurrencyRun(recorded, concurrencyReplayID(t, recorded)); err == nil {
				t.Fatalf("%s replayed with its release moved: it does not pin the release", f.path())
			}
		})
	}
}

// recordedAcquires is every AcquireConcurrency local activity's result, in
// order.
func recordedAcquires(t *testing.T, recorded *historypb.History) []engine.AcquireConcurrencyResult {
	t.Helper()
	var out []engine.AcquireConcurrencyResult
	for _, event := range recorded.GetEvents() {
		attrs := event.GetMarkerRecordedEventAttributes()
		if attrs == nil || attrs.GetMarkerName() != "LocalActivity" {
			continue
		}
		var data struct{ ActivityType string }
		must.OKf(t, converter.GetDefaultDataConverter().FromPayloads(attrs.GetDetails()["data"], &data), "decode a marker")
		if data.ActivityType != engine.ActivityAcquireConcurrency {
			continue
		}
		var result engine.AcquireConcurrencyResult
		must.OKf(t, converter.GetDefaultDataConverter().FromPayloads(attrs.GetDetails()["result"], &result), "decode an acquire's result")
		out = append(out, result)
	}
	if len(out) == 0 {
		t.Fatal("the fixture records no acquire")
	}
	return out
}

// failedSnapshotWrites is which of the recorded run's snapshot writes, by
// their order, failed: an emit whose snapshot could not be written ends the
// run.
func failedSnapshotWrites(t *testing.T, recorded *historypb.History) map[int]bool {
	t.Helper()
	out, n := map[int]bool{}, 0
	for _, event := range recorded.GetEvents() {
		attrs := event.GetMarkerRecordedEventAttributes()
		if attrs == nil || attrs.GetMarkerName() != "LocalActivity" {
			continue
		}
		var data struct{ ActivityType string }
		must.OKf(t, converter.GetDefaultDataConverter().FromPayloads(attrs.GetDetails()["data"], &data), "decode a marker")
		if data.ActivityType != engine.ActivityWriteSnapshot {
			continue
		}
		if attrs.GetFailure() != nil {
			out[n] = true
		}
		n++
	}
	return out
}

// recordedRunSignals is when, after its start, the run was granted or asked
// to cancel, as the recorded run took them.
type runSignal struct {
	at     time.Duration
	grant  *engine.ConcurrencyGrant
	cancel bool
}

func recordedRunSignals(t *testing.T, recorded *historypb.History) []runSignal {
	t.Helper()
	events := recorded.GetEvents()
	start := events[0].GetEventTime().AsTime()
	var out []runSignal
	for _, event := range events {
		at := event.GetEventTime().AsTime().Sub(start)
		if attrs := event.GetWorkflowExecutionSignaledEventAttributes(); attrs != nil && attrs.GetSignalName() == engine.ConcurrencyGrantSignal {
			var grant engine.ConcurrencyGrant
			must.OKf(t, converter.GetDefaultDataConverter().FromPayloads(attrs.GetInput(), &grant), "decode a grant")
			out = append(out, runSignal{at: at, grant: &grant})
		}
		if event.GetWorkflowExecutionCancelRequestedEventAttributes() != nil {
			out = append(out, runSignal{at: at, cancel: true})
		}
	}
	return out
}

// recordedReleases is every release_all the recorded run sent, as JSON.
func recordedReleases(t *testing.T, recorded *historypb.History) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, event := range recorded.GetEvents() {
		if attrs := event.GetSignalExternalWorkflowExecutionInitiatedEventAttributes(); attrs != nil &&
			attrs.GetSignalName() == engine.GateSignalReleaseAll {
			out = append(out, recordedPayload(t, attrs.GetInput(), 0))
		}
	}
	return out
}

// failedPublishes is each event type whose publish the recorded run saw
// fail, with the failure's type.
func failedPublishes(t *testing.T, recorded *historypb.History) map[string]string {
	t.Helper()
	scheduled := map[int64]string{}
	out := map[string]string{}
	for _, event := range recorded.GetEvents() {
		if attrs := event.GetActivityTaskScheduledEventAttributes(); attrs != nil && attrs.GetActivityType().GetName() == engine.ActivityPublishEvent {
			typ, _ := recordedPayload(t, attrs.GetInput(), 0)["type"].(string)
			scheduled[event.GetEventId()] = typ
		}
		if attrs := event.GetActivityTaskFailedEventAttributes(); attrs != nil {
			if typ, ok := scheduled[attrs.GetScheduledEventId()]; ok {
				out[typ] = attrs.GetFailure().GetApplicationFailureInfo().GetType()
			}
		}
	}
	return out
}

// An event's message quotes wall clocks (a queue's deadline, how long a run
// waited) and, when a snapshot write failed, the store's own error, which a
// mocked write cannot reproduce. clocklessEvent sets both aside, keeping the
// engine's own words around them.
var (
	eventInstant      = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z`)
	eventWaited       = regexp.MustCompile(`waited (\S+) for`)
	eventSnapshotFail = regexp.MustCompile(`(write snapshot for event \d+): .*`)
)

// bySequence orders encoded events by their sequence.
func bySequence(t *testing.T, events []map[string]any) {
	t.Helper()
	seq := func(e map[string]any) int64 {
		n, err := strconv.ParseInt(fmt.Sprint(e["sequence"]), 10, 64)
		must.OKf(t, err, "an encoded event's sequence")
		return n
	}
	slices.SortStableFunc(events, func(a, b map[string]any) int { return cmp.Compare(seq(a), seq(b)) })
}

// clocksAgree holds the instants and durations clocklessEvent sets aside to
// what the recorded build sent, within the moments between the recorded
// start and the workflow task that read the clock.
func clocksAgree(recorded, current map[string]any) error {
	const within = 2 * time.Second
	rec, _ := recorded["message"].(string)
	cur, _ := current["message"].(string)
	ri, ci := eventInstant.FindAllString(rec, -1), eventInstant.FindAllString(cur, -1)
	if len(ri) != len(ci) {
		return fmt.Errorf("%d instants, the recorded event %d", len(ci), len(ri))
	}
	for i := range ri {
		r, err := time.Parse(time.RFC3339Nano, ri[i])
		if err != nil {
			return err
		}
		c, err := time.Parse(time.RFC3339Nano, ci[i])
		if err != nil {
			return err
		}
		if d := c.Sub(r).Abs(); d > within {
			return fmt.Errorf("instant %s, the recorded %s", ci[i], ri[i])
		}
	}
	rw, cw := eventWaited.FindAllStringSubmatch(rec, -1), eventWaited.FindAllStringSubmatch(cur, -1)
	if len(rw) != len(cw) {
		return fmt.Errorf("%d waits, the recorded event %d", len(cw), len(rw))
	}
	for i := range rw {
		r, err := time.ParseDuration(rw[i][1])
		if err != nil {
			return err
		}
		c, err := time.ParseDuration(cw[i][1])
		if err != nil {
			return err
		}
		if d := (c - r).Abs(); d > within {
			return fmt.Errorf("waited %s, the recorded %s", c, r)
		}
	}
	// The wait an event ends (waitEnd.waited) is the run's clock, which the
	// re-execution does not reproduce: a publish's retries, mocked, take no
	// time. Both must carry a duration; its value is the recording's.
	recEnd, _ := recorded["waitEnd"].(map[string]any)
	curEnd, _ := current["waitEnd"].(map[string]any)
	if (recEnd == nil) != (curEnd == nil) {
		return fmt.Errorf("waitEnd %v, the recorded %v", curEnd, recEnd)
	}
	if recEnd != nil {
		_, rerr := time.ParseDuration(fmt.Sprint(recEnd["waited"]))
		_, cerr := time.ParseDuration(fmt.Sprint(curEnd["waited"]))
		if rerr != nil || cerr != nil {
			return fmt.Errorf("waitEnd.waited %v, the recorded %v", curEnd["waited"], recEnd["waited"])
		}
	}
	return nil
}

func clocklessEvent(event map[string]any) map[string]any {
	message, ok := event["message"].(string)
	if !ok {
		return event
	}
	out := maps.Clone(event)
	if end, ok := event["waitEnd"].(map[string]any); ok {
		end = maps.Clone(end)
		end["waited"] = "<duration>"
		out["waitEnd"] = end
	}
	message = eventInstant.ReplaceAllString(message, "<instant>")
	message = eventWaited.ReplaceAllString(message, "waited <duration> for")
	out["message"] = eventSnapshotFail.ReplaceAllString(message, "$1: <the store's error>")
	return out
}

// TestConcurrencyRunHistories_PayloadsMatchWhatCurrentCodeSends is the
// payload half: each single-generation fixture is re-executed from its own
// input, with its recorded acquire answers, grants, cancel, task results and
// publish failures fed back, and every payload current code sends (task
// inputs, events, the release) is compared with what the recorded build
// sent. Wall clocks (an event's instants and waits, and the wait an event
// ends), and a failed write's store error, are set aside (clocklessEvent);
// clocksAgree holds the instants and waits of a message within two seconds
// of the recording, and only the presence of the wait an event ends.
func TestConcurrencyRunHistories_PayloadsMatchWhatCurrentCodeSends(t *testing.T) {
	for _, f := range concurrencyRunFixtures {
		if !f.payload {
			continue
		}
		t.Run(f.name, func(t *testing.T) {
			requireConcurrencyRunFixture(t, f)
			recorded := loadHistoryFile(t, f.path())
			runID, wf, input, plan := goldenWorkflowArgs(t, recorded)
			answers := recordedAcquires(t, recorded)
			signals := recordedRunSignals(t, recorded)
			releases := recordedReleases(t, recorded)
			failing := failedPublishes(t, recorded)
			refused := failedSnapshotWrites(t, recorded)
			writes := 0
			recordedTasks, recordedEvents, recordedResults := concurrencyActivityPayloads(t, recorded)

			var (
				mu        sync.Mutex
				asked     int
				sentTasks = map[string]map[string]any{}
				sentEvent []map[string]any
				sentRel   []map[string]any
			)
			suite := &testsuite.WorkflowTestSuite{}
			env := generousDeadlock(suite.NewTestWorkflowEnvironment())
			env.SetTestTimeout(60 * time.Second)
			// The run's clock starts where the recorded one did, so a deadline
			// it computes from now is comparable with the recorded one.
			env.SetStartTime(recorded.GetEvents()[0].GetEventTime().AsTime())
			env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: runID})
			env.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
			env.RegisterActivityWithOptions(func(context.Context, engine.AcquireConcurrencyInput) (engine.AcquireConcurrencyResult, error) {
				mu.Lock()
				defer mu.Unlock()
				a := answers[min(asked, len(answers)-1)]
				asked++
				return a, nil
			}, activity.RegisterOptions{Name: engine.ActivityAcquireConcurrency})
			env.RegisterActivityWithOptions(func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
				mu.Lock()
				defer mu.Unlock()
				sentTasks[in.GetTaskId()] = canonicalPayload(t, in)
				result, ok := recordedResults[in.GetTaskId()]
				if !ok {
					return nil, fmt.Errorf("current code ran task %q, which the recorded run never executed", in.GetTaskId())
				}
				return result, nil
			}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
			env.RegisterActivityWithOptions(func(_ context.Context, run *orkstackv1.Run, sequence int64) (string, error) {
				mu.Lock()
				defer mu.Unlock()
				i := writes
				writes++
				if refused[i] {
					return "", temporal.NewNonRetryableApplicationError("the recorded snapshot write failed", "SNAPSHOT_REFUSED", nil)
				}
				return fmt.Sprintf("memory://snapshots/%s/%d.pb", run.GetId(), sequence), nil
			}, activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
			env.RegisterActivityWithOptions(func(_ context.Context, event *orkstackv1.Event, _, _ string) error {
				mu.Lock()
				defer mu.Unlock()
				encoded := canonicalPayload(t, event)
				sentEvent = append(sentEvent, encoded)
				if typ, fail := failing[encoded["type"].(string)]; fail {
					return temporal.NewNonRetryableApplicationError("the recorded publish failed", typ, nil)
				}
				return nil
			}, activity.RegisterOptions{Name: engine.ActivityPublishEvent})
			env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, "", engine.GateSignalReleaseAll, mock.Anything).Return(
				func(_, _, _, _ string, arg any) error {
					mu.Lock()
					defer mu.Unlock()
					sentRel = append(sentRel, canonicalPayload(t, arg))
					return nil
				})
			for _, sig := range signals {
				env.RegisterDelayedCallback(func() {
					if sig.cancel {
						env.CancelWorkflow()
						return
					}
					env.SignalWorkflow(engine.ConcurrencyGrantSignal, *sig.grant)
				}, max(sig.at, time.Millisecond))
			}
			env.ExecuteWorkflow(engine.RunWorkflow, runID, wf, input, plan, (*orkstackv1.RunContinuation)(nil))
			if !env.IsWorkflowCompleted() {
				t.Fatal("re-executing the fixture did not end the run")
			}

			for id, want := range recordedTasks {
				if diff := payloadDiff(want, sentTasks[id]); diff != "" {
					t.Errorf("task %s's input differs from what the recorded build sent:\n%s", id, diff)
				}
			}
			if len(sentTasks) != len(recordedTasks) {
				t.Errorf("current code ran %d tasks, the recorded run %d", len(sentTasks), len(recordedTasks))
			}
			// The publish window keeps several publishes in flight, so they
			// reach the mock in any order: compare them by sequence.
			bySequence(t, recordedEvents)
			bySequence(t, sentEvent)
			if len(sentEvent) != len(recordedEvents) {
				t.Fatalf("current code published %d events, the recorded run %d", len(sentEvent), len(recordedEvents))
			}
			for i := range recordedEvents {
				if err := clocksAgree(recordedEvents[i], sentEvent[i]); err != nil {
					t.Errorf("event %d's clocks differ from what the recorded build published: %v", i, err)
				}
				if diff := payloadDiff(clocklessEvent(recordedEvents[i]), clocklessEvent(sentEvent[i])); diff != "" {
					t.Errorf("event %d differs from what the recorded build published:\n%s", i, diff)
				}
			}
			if len(sentRel) != len(releases) {
				t.Fatalf("current code sent %d releases, the recorded run %d", len(sentRel), len(releases))
			}
			for i := range releases {
				if diff := payloadDiff(releases[i], sentRel[i]); diff != "" {
					t.Errorf("release %d differs from what the recorded build sent:\n%s", i, diff)
				}
			}
		})
	}
}

// concurrencyActivityPayloads is goldenActivityPayloads for a run that may
// end before its first task: the task inputs and results it has, and every
// event it published.
func concurrencyActivityPayloads(t *testing.T, recorded *historypb.History) (
	tasks map[string]map[string]any, events []map[string]any, results map[string]*orkstackv1.ExecuteTaskResult,
) {
	t.Helper()
	tasks, results = map[string]map[string]any{}, map[string]*orkstackv1.ExecuteTaskResult{}
	byScheduled := map[int64]string{}
	for _, event := range recorded.GetEvents() {
		if attrs := event.GetActivityTaskScheduledEventAttributes(); attrs != nil {
			switch attrs.GetActivityType().GetName() {
			case task.ActivityExecuteTask:
				decoded := recordedPayload(t, attrs.GetInput(), 0)
				id, _ := decoded["taskId"].(string)
				tasks[id], byScheduled[event.GetEventId()] = decoded, id
			case engine.ActivityPublishEvent:
				events = append(events, recordedPayload(t, attrs.GetInput(), 0))
			}
		}
		if attrs := event.GetActivityTaskCompletedEventAttributes(); attrs != nil {
			if id, ok := byScheduled[attrs.GetScheduledEventId()]; ok {
				var result *orkstackv1.ExecuteTaskResult
				must.OKf(t, converter.GetDefaultDataConverter().FromPayloads(attrs.GetResult(), &result), "decode a task result")
				results[id] = result
			}
		}
	}
	if len(events) == 0 {
		t.Fatal("the fixture published no event")
	}
	return tasks, events, results
}
