//go:build integration

package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/localcatalog"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The join-policy integration suite (docs/DSL.md "Joins and failure handlers"): real
// Temporal, real MinIO and real Orkers running the first-party shell image
// in Docker, on examples/branch-and-recover.ork.yaml AS SHIPPED — driven only
// by its inputs — except the two named variants, each defined by the one
// change it makes to rollback (a no-retry variant and an await variant).
// A phase the test must act inside is held open by the example's own
// smoke_seconds / rollback_seconds, and the test acts when it OBSERVES the
// moment — an event, a live attempt's heartbeat — never after a sleep of its
// own. Every scenario runs in a private Temporal namespace on private
// channels (AGENTS.md §11.1).

const branchAndRecover = "../../examples/branch-and-recover.ork.yaml"

// joinE2E is the cache suite's deployment (a run worker, real Orkers with a
// memo store, an event worker) plus the local activity catalog the shipped
// example's `uses: shell` resolves through.
type joinE2E struct {
	*cacheE2E
	catalog dsl.Catalog
	mu      sync.Mutex
	invoked map[string]int // ExecuteTask invocations per run/task
	// orkerActivities and orkerClients are every Orker this suite started,
	// by id.
	orkerActivities map[string]*orker.Activities
	orkerClients    map[string]client.Client
}

func startJoinE2E(t *testing.T) *joinE2E {
	t.Helper()
	c := startCacheE2E(t)
	env, err := localcatalog.CatalogEnv()
	if err != nil {
		infraAbsent(t, "%v", err)
	}
	catalog, err := dsl.ParseCatalog([]byte(strings.TrimPrefix(env, "ORK_ACTIVITY_CATALOG=")))
	if err != nil {
		t.Fatalf("parse the local catalog: %v", err)
	}
	return &joinE2E{cacheE2E: c, catalog: catalog, invoked: map[string]int{},
		orkerActivities: map[string]*orker.Activities{}, orkerClients: map[string]client.Client{}}
}

// orker starts one real Orker, with the memo store, that counts every
// ExecuteTask invocation per run and task.
func (j *joinE2E) orker(id string) worker.Worker {
	j.t.Helper()
	refs, _ := j.store.(artifacts.RefPredictor)
	activities := orker.NewActivitiesWithOptions(j.store, orker.Options{
		Logs: j.logs, Commits: j.commits, Attempts: j.attempts, Refs: refs, Memos: j.memos,
		Pool: task.DefaultPool, OrkerID: id, WorkRoot: j.workRoot,
	})
	if err := activities.Boot(j.ctx); err != nil {
		infraAbsent(j.t, "orker boot checks failed: %v", err)
	}
	// This Orker's OWN client: killing it closes the client before the
	// worker stops, so the stop can never sneak a completion out ahead of the
	// death (pin_affinity_integration_test.go's pinOrker.kill).
	own, err := client.DialContext(j.ctx, client.Options{HostPort: temporalAddress(), Namespace: j.namespace, Identity: id})
	if err != nil {
		infraAbsent(j.t, "orker %s cannot dial temporal: %v", id, err)
	}
	j.t.Cleanup(func() { j.closeClient(id) })
	w := worker.New(own, j.orkerQueue, worker.Options{
		DisableRegistrationAliasing: true, DisableWorkflowWorker: true, Identity: id,
	})
	w.RegisterActivityWithOptions(func(actx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		j.mu.Lock()
		j.invoked[in.GetRunId()+"/"+in.GetTaskId()]++
		j.mu.Unlock()
		return activities.ExecuteTask(actx, in)
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	w.RegisterActivityWithOptions(activities.DescribeOrker, activity.RegisterOptions{Name: task.ActivityDescribeOrker})
	if err := w.Start(); err != nil {
		j.t.Fatalf("start orker %s: %v", id, err)
	}
	j.t.Cleanup(w.Stop)
	j.mu.Lock()
	j.orkerActivities[id] = activities
	j.orkerClients[id] = own
	j.mu.Unlock()
	return w
}

func (j *joinE2E) closeClient(id string) {
	j.mu.Lock()
	c := j.orkerClients[id]
	delete(j.orkerClients, id)
	j.mu.Unlock()
	if c != nil {
		c.Close()
	}
}

// kill is a host death, not a drain: the Orker's client is closed first, so
// nothing it was running can report back, then its worker stops.
func (j *joinE2E) kill(id string, w worker.Worker) {
	j.closeClient(id)
	w.Stop()
	j.reap(id)
}

// reap removes, best effort, every container Orker id left behind — a
// stopped worker's container outlives it.
func (j *joinE2E) reap(id string) {
	j.mu.Lock()
	activities := j.orkerActivities[id]
	j.mu.Unlock()
	if activities == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := activities.Dispatcher().Runtime.Reap(ctx, id); err != nil {
		j.t.Logf("reap %s's containers: %v", id, err)
	}
}

func (j *joinE2E) invocations(runID, taskID string) int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.invoked[runID+"/"+taskID]
}

// noRetryVariant is the shipped file with rollback's retry: line removed.
func noRetryVariant(src string) string {
	return strings.Replace(src, "    join: any_failed\n    retry: { maximum_attempts: 2 }\n", "    join: any_failed\n", 1)
}

// awaitVariant replaces the whole rollback task with an awaited one.
func awaitVariant(src string) string {
	start := strings.Index(src, "  - id: rollback\n")
	end := strings.Index(src, "  - id: escalate\n")
	return src[:start] + "  - id: rollback\n    needs: [deploy]\n    join: any_failed\n    await: {timeout_seconds: 600}\n\n" + src[end:]
}

// compileDoc compiles source against the local catalog, resolves every
// image as apply does, and stamps the digest.
func (j *joinE2E) compileDoc(source string) *orkstackv1.Orkflow {
	j.t.Helper()
	wf, err := dsl.CompileWithOptions([]byte(source), dsl.Options{Catalog: j.catalog})
	if err != nil {
		j.t.Fatalf("compile: %v", err)
	}
	if _, err := dsl.NewResolver("").ResolveOrkflow(j.ctx, j.catalog, wf); err != nil {
		infraAbsent(j.t, "resolve the local activity images: %v", err)
	}
	digest, err := dsl.DefinitionDigest(wf)
	if err != nil {
		j.t.Fatal(err)
	}
	wf.DefinitionDigest = digest
	return wf
}

// example is the shipped example, as applied, after variant (nil: as shipped).
func (j *joinE2E) example(variant func(string) string) *orkstackv1.Orkflow {
	j.t.Helper()
	raw, err := os.ReadFile(branchAndRecover)
	if err != nil {
		j.t.Fatal(err)
	}
	source := string(raw)
	if variant != nil {
		source = variant(source)
	}
	return j.compileDoc(source)
}

func (j *joinE2E) startWith(runID string, wf *orkstackv1.Orkflow, input map[string]any, plan engine.RunPlan) {
	j.t.Helper()
	data, err := structpb.NewStruct(input)
	if err != nil {
		j.t.Fatal(err)
	}
	starter := engine.NewTemporalStarterWithOrkerQueue(j.temporal, j.runQueue, j.orkerQueue)
	if err := starter.StartRun(j.ctx, runID, wf, &orkstackv1.Payload{Data: data}, plan); err != nil {
		j.t.Fatalf("StartRun: %v", err)
	}
}

func (j *joinE2E) startRun(runID string, wf *orkstackv1.Orkflow, input map[string]any) {
	j.startWith(runID, wf, input, j.cachePlan(false))
}

func joinRunID(label string) string {
	return fmt.Sprintf("ork-join-%s-%d", label, time.Now().UnixNano())
}

// awaitEvent blocks, by deadline, until the run has published an event
// matching match.
func (j *joinE2E) awaitEvent(runID string, within time.Duration, what string, match func(*orkstackv1.Event) bool) *orkstackv1.Event {
	j.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		for _, e := range j.bus.Events(runID, 0) {
			if match(e) {
				return e
			}
		}
		select {
		case <-j.ctx.Done():
			j.t.Fatalf("context ended waiting for %s", what)
		case <-time.After(100 * time.Millisecond):
		}
	}
	j.t.Fatalf("run %s never published %s within %s", runID, what, within)
	return nil
}

func taskEvent(taskID string, eventType orkstackv1.EventType) func(*orkstackv1.Event) bool {
	return func(e *orkstackv1.Event) bool { return e.GetTaskId() == taskID && e.GetType() == eventType }
}

func (j *joinE2E) events(runID string) []*orkstackv1.Event { return j.bus.Events(runID, 0) }

func (j *joinE2E) eventMessage(runID, taskID string, status orkstackv1.Status) string {
	for _, e := range j.events(runID) {
		if e.GetTaskId() == taskID && e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_LOG && e.GetStatus() == status {
			return e.GetMessage()
		}
	}
	return ""
}

func (j *joinE2E) attemptsOf(runID, taskID string) []*orkstackv1.TaskAttempt {
	j.t.Helper()
	records, err := j.attempts.ListAttempts(j.ctx, runID, taskID, artifacts.NoItem)
	if err != nil {
		j.t.Fatalf("list %s's attempts: %v", taskID, err)
	}
	return records
}

// attemptsLanded is attemptsOf once at least n records have landed, or
// whatever has when within passes. A canceled attempt's record is the
// Orker's last write for it, made after it acknowledged the cancel, so the
// run can end before the record lands; a read right after the run ends can
// see none.
func (j *joinE2E) attemptsLanded(runID, taskID string, n int, within time.Duration) []*orkstackv1.TaskAttempt {
	j.t.Helper()
	deadline := time.Now().Add(within)
	for {
		records := j.attemptsOf(runID, taskID)
		if len(records) >= n || time.Now().After(deadline) {
			return records
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func statusIs(t *testing.T, run *orkstackv1.Run, taskID string, want orkstackv1.Status) {
	t.Helper()
	if got := taskRunNamed(run, taskID).GetStatus(); got != want {
		t.Errorf("%s = %v, want %v (%v)", taskID, got, want, taskRunNamed(run, taskID).GetError())
	}
}

func runLine(run *orkstackv1.Run, ids ...string) string {
	var parts []string
	for _, id := range ids {
		parts = append(parts, fmt.Sprintf("%s=%s", id,
			strings.TrimPrefix(taskRunNamed(run, id).GetStatus().String(), "STATUS_")))
	}
	return strings.Join(parts, " ")
}

var allTasks = []string{"decide", "canary", "full", "verify", "deploy", "smoke", "announce", "rollback", "escalate", "notify"}

const anyFailedSkip = "task skipped: join any_failed: none of its needs failed (deploy); it runs only when one of them fails"

// Re-join, both routes.
func TestIntegration_JoinRejoinsAfterTheBranchThatRan(t *testing.T) {
	j := startJoinE2E(t)
	j.orker("join-i1")
	wf := j.example(nil)
	var lines []string
	for _, route := range []string{"canary", "full"} {
		runID := joinRunID("i1-" + route)
		input := map[string]any{"route": route, "smoke_seconds": 1}
		j.startRun(runID, wf, input)
		run := j.wait(runID)
		if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Fatalf("%s run = %v %v", route, run.GetStatus(), run.GetError())
		}
		other := map[string]string{"canary": "full", "full": "canary"}[route]
		for _, id := range []string{"decide", route, "verify", "deploy", "smoke", "announce", "notify"} {
			statusIs(t, run, id, orkstackv1.Status_STATUS_SUCCEEDED)
		}
		statusIs(t, run, other, orkstackv1.Status_STATUS_SKIPPED)
		if got := j.eventMessage(runID, "rollback", orkstackv1.Status_STATUS_SKIPPED); got != anyFailedSkip {
			t.Errorf("rollback skip = %q", got)
		}
		if got := j.eventMessage(runID, "escalate", orkstackv1.Status_STATUS_SKIPPED); !strings.Contains(got, "none of its needs failed (rollback)") {
			t.Errorf("escalate skip = %q", got)
		}
		// The plan of the same input agrees with the run on every task.
		inputPayload, _ := structpb.NewStruct(input)
		value, _ := structpb.NewStruct(map[string]any{"value": route})
		plan, err := engine.Simulate(wf, &orkstackv1.Payload{Data: inputPayload},
			map[string]*orkstackv1.Payload{"decide": {Data: value}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range plan.GetTasks() {
			got := taskRunNamed(run, p.GetTaskId()).GetStatus()
			want := map[orkstackv1.PlanOutcome]orkstackv1.Status{
				orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS:    orkstackv1.Status_STATUS_SUCCEEDED,
				orkstackv1.PlanOutcome_PLAN_OUTCOME_SKIPPED: orkstackv1.Status_STATUS_SKIPPED,
			}[p.GetOutcome()]
			if got != want {
				t.Errorf("%s: %s planned %v, run %v", route, p.GetTaskId(), p.GetOutcome(), got)
			}
		}
		lines = append(lines, fmt.Sprintf("route=%s run=%s %s", route, runID, runLine(run, allTasks...)))
	}
	proof.Write(t, "engine", "join-I1.txt", []byte(strings.Join(lines, "\n")+"\n"))
}

// Every branch skipped.
func TestIntegration_JoinEveryBranchSkipped(t *testing.T) {
	j := startJoinE2E(t)
	j.orker("join-i2")
	runID := joinRunID("i2")
	j.startRun(runID, j.example(nil), map[string]any{"route": "none"})
	run := j.wait(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	for _, id := range []string{"canary", "full", "verify", "deploy", "smoke", "announce", "rollback", "escalate"} {
		statusIs(t, run, id, orkstackv1.Status_STATUS_SKIPPED)
	}
	if got := j.eventMessage(runID, "verify", orkstackv1.Status_STATUS_SKIPPED); got !=
		"task skipped: join none_failed: every need was skipped (canary, full)" {
		t.Errorf("verify skip = %q", got)
	}
	statusIs(t, run, "notify", orkstackv1.Status_STATUS_SUCCEEDED)
	stdout := j.attemptLog(runID, "notify", 1)
	for _, want := range []string{"deploy: SKIPPED", "smoke: SKIPPED"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("notify's stdout lacks %q:\n%s", want, stdout)
		}
	}
	proof.Write(t, "engine", "join-I2.txt", []byte(runLine(run, allTasks...)+"\n"+stdout))
}

// failingNotes is every failing note of a run, in order.
func failingNotes(events []*orkstackv1.Event) []*orkstackv1.Event {
	var out []*orkstackv1.Event
	for _, e := range events {
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_LOG && e.GetTaskId() == "" {
			out = append(out, e)
		}
	}
	return out
}

// A failure handler.
func TestIntegration_JoinFailureHandlerRunsAndTheRunStillFails(t *testing.T) {
	j := startJoinE2E(t)
	j.orker("join-i3")
	runID := joinRunID("i3")
	j.startRun(runID, j.example(nil), map[string]any{"route": "canary", "fail_deploy": true, "smoke_seconds": 120})
	run := j.wait(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	deploy := taskRunNamed(run, "deploy")
	if deploy.GetStatus() != orkstackv1.Status_STATUS_FAILED || len(j.attemptsOf(runID, "deploy")) != 2 {
		t.Fatalf("deploy = %v with %d attempt records", deploy.GetStatus(), len(j.attemptsOf(runID, "deploy")))
	}
	if run.GetError().GetDetails() != `task "deploy"` || run.GetError().GetCode() != "RUN_FAILED" {
		t.Fatalf("Run.error = %v", run.GetError())
	}
	statusIs(t, run, "smoke", orkstackv1.Status_STATUS_CANCELED)
	smokeAttempts := j.attemptsOf(runID, "smoke")
	if len(smokeAttempts) != 1 || smokeAttempts[0].GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_CANCELED {
		t.Errorf("smoke's attempt records = %v, want one CANCELED", smokeAttempts)
	}
	announce := taskRunNamed(run, "announce")
	if announce.GetStatus() != orkstackv1.Status_STATUS_CANCELED || announce.GetStartedAt() != nil ||
		j.eventMessage(runID, "announce", orkstackv1.Status_STATUS_CANCELED) !=
			`task canceled: the run is failing (task "deploy" failed) and this task is not a failure handler` {
		t.Errorf("announce = %v %q", announce, j.eventMessage(runID, "announce", orkstackv1.Status_STATUS_CANCELED))
	}
	statusIs(t, run, "rollback", orkstackv1.Status_STATUS_SUCCEEDED)
	if n := len(j.attemptsOf(runID, "rollback")); n != 1 {
		t.Errorf("rollback has %d attempt records, want 1", n)
	}
	rollbackOut := j.attemptLog(runID, "rollback", 1)
	for _, want := range []string{`"policy":"any_failed"`, `"taskId":"deploy","status":"FAILED"`, "deploying route canary",
		"deploy failed: the health check did not pass"} {
		if !strings.Contains(rollbackOut, want) {
			t.Errorf("rollback's stdout lacks %q:\n%s", want, rollbackOut)
		}
	}
	statusIs(t, run, "escalate", orkstackv1.Status_STATUS_SKIPPED)
	statusIs(t, run, "notify", orkstackv1.Status_STATUS_SUCCEEDED)
	notifyOut := j.attemptLog(runID, "notify", 1)
	for _, want := range []string{"deploy: FAILED (", "smoke: CANCELED (TASK_CANCELED)"} {
		if !strings.Contains(notifyOut, want) {
			t.Errorf("notify's stdout lacks %q:\n%s", want, notifyOut)
		}
	}
	events := j.events(runID)
	notes := failingNotes(events)
	if len(notes) != 1 || notes[0].GetStatus() != orkstackv1.Status_STATUS_RUNNING {
		t.Fatalf("failing notes = %v", notes)
	}
	var deployFailedSeq, smokeCanceledSeq, lastHandlerSeq int64
	for _, e := range events {
		switch {
		case e.GetTaskId() == "deploy" && e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_FAILED:
			deployFailedSeq = e.GetSequence()
		case e.GetTaskId() == "smoke" && e.GetStatus() == orkstackv1.Status_STATUS_CANCELED:
			smokeCanceledSeq = e.GetSequence()
		case e.GetTaskId() == "rollback" || e.GetTaskId() == "notify" || e.GetTaskId() == "escalate":
			lastHandlerSeq = e.GetSequence()
		}
	}
	if notes[0].GetSequence() < deployFailedSeq || notes[0].GetSequence() < smokeCanceledSeq {
		t.Errorf("failing note %d before deploy's failure %d or smoke's cancel %d", notes[0].GetSequence(), deployFailedSeq, smokeCanceledSeq)
	}
	last := events[len(events)-1]
	if last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED || last.GetSequence() < lastHandlerSeq {
		t.Errorf("last event = %v #%d, last handler event #%d", last.GetType(), last.GetSequence(), lastHandlerSeq)
	}
	runJSON, _ := json.Marshal(map[string]any{
		"deploy":   map[string]any{"status": deploy.GetStatus().String()},
		"rollback": map[string]any{"status": taskRunNamed(run, "rollback").GetStatus().String(), "join": taskRunNamed(run, "rollback").GetJoin().String()},
		"error":    run.GetError(),
	})
	noteJSON, _ := json.Marshal(notes[0])
	proof.Write(t, "engine", "join-I3.txt", []byte(fmt.Sprintf("run=%s\n%s\n%s\nfailing-note=%s\nrollback-stdout:\n%s\nnotify-stdout:\n%s\n",
		runID, runLine(run, allTasks...), runJSON, noteJSON, rollbackOut, notifyOut)))
}

// The failing shape on the wire, from a second replica's reader.
func TestIntegration_JoinFailingShapeOnTheWire(t *testing.T) {
	j := startJoinE2E(t)
	j.orker("join-i4")
	runID := joinRunID("i4")
	j.startRun(runID, j.example(nil), map[string]any{"route": "full", "fail_deploy": true, "smoke_seconds": 1, "rollback_seconds": 30})
	j.awaitEvent(runID, 5*time.Minute, "rollback's TASK_STARTED", taskEvent("rollback", orkstackv1.EventType_EVENT_TYPE_TASK_STARTED))
	replica := engine.NewTemporalRunReader(j.temporal).WithSnapshotStore(j.snapshots)
	failing, err := replica.GetRun(j.ctx, runID)
	if err != nil {
		t.Fatalf("second replica GetRun: %v", err)
	}
	if failing.GetStatus() != orkstackv1.Status_STATUS_RUNNING || failing.GetError().GetDetails() != `task "deploy"` ||
		failing.GetFinishedAt() != nil {
		t.Fatalf("failing run on the wire = %v / %v", failing.GetStatus(), failing.GetError())
	}
	final := j.wait(runID)
	ended, err := replica.GetRun(j.ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if ended.GetStatus() != orkstackv1.Status_STATUS_FAILED || ended.GetError().GetMessage() != failing.GetError().GetMessage() {
		t.Fatalf("ended = %v / %v, failing error %v", ended.GetStatus(), ended.GetError(), failing.GetError())
	}
	_ = final
	proof.Write(t, "engine", "join-I4.txt", []byte(fmt.Sprintf("while failing: status=%s error=%q\nended: status=%s error=%q\n",
		failing.GetStatus(), failing.GetError().GetMessage(), ended.GetStatus(), ended.GetError().GetMessage())))
}

// A handler fails.
func TestIntegration_JoinAHandlerFailsAndEscalates(t *testing.T) {
	j := startJoinE2E(t)
	j.orker("join-i5")
	runID := joinRunID("i5")
	j.startRun(runID, j.example(nil), map[string]any{"route": "canary", "fail_deploy": true, "fail_rollback": true, "smoke_seconds": 1})
	run := j.wait(runID)
	statusIs(t, run, "rollback", orkstackv1.Status_STATUS_FAILED)
	records := j.attemptsOf(runID, "rollback")
	if len(records) != 2 {
		t.Fatalf("rollback has %d attempt records, want 2", len(records))
	}
	for _, r := range records {
		if r.GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED {
			t.Errorf("rollback attempt %d = %v", r.GetAttempt(), r.GetOutcome())
		}
	}
	statusIs(t, run, "escalate", orkstackv1.Status_STATUS_SUCCEEDED)
	statusIs(t, run, "notify", orkstackv1.Status_STATUS_SUCCEEDED)
	var rollbackFailed, escalateScheduled []int64
	for _, e := range j.events(runID) {
		if e.GetTaskId() == "rollback" && e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_FAILED {
			rollbackFailed = append(rollbackFailed, e.GetSequence())
		}
		if e.GetTaskId() == "escalate" && e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED {
			escalateScheduled = append(escalateScheduled, e.GetSequence())
		}
	}
	if len(rollbackFailed) != 1 || len(escalateScheduled) != 1 || escalateScheduled[0] < rollbackFailed[0] {
		t.Fatalf("rollback TASK_FAILED %v, escalate TASK_SCHEDULED %v", rollbackFailed, escalateScheduled)
	}
	if run.GetError().GetDetails() != `task "deploy"` {
		t.Fatalf("Run.error = %v, want deploy's", run.GetError())
	}
	proof.Write(t, "engine", "join-I5.txt", []byte(runLine(run, allTasks...)+"\n"))
}

// Operator cancel before any failure.
func TestIntegration_JoinOperatorCancelStartsNoHandler(t *testing.T) {
	j := startJoinE2E(t)
	j.orker("join-i6")
	runID := joinRunID("i6")
	j.startRun(runID, j.example(nil), map[string]any{"route": "canary", "smoke_seconds": 120})
	j.awaitEvent(runID, 5*time.Minute, "smoke's TASK_STARTED", taskEvent("smoke", orkstackv1.EventType_EVENT_TYPE_TASK_STARTED))
	if err := j.temporal.CancelWorkflow(j.ctx, runID, ""); err != nil {
		t.Fatal(err)
	}
	run := j.wait(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("run = %v", run.GetStatus())
	}
	sentence := "task canceled: the run was canceled; a canceled run starts no failure handler"
	if got := j.eventMessage(runID, "notify", orkstackv1.Status_STATUS_CANCELED); got != sentence {
		t.Errorf("notify = %q", got)
	}
	for _, id := range []string{"rollback", "escalate"} {
		tr := taskRunNamed(run, id)
		switch tr.GetStatus() {
		case orkstackv1.Status_STATUS_SKIPPED:
		case orkstackv1.Status_STATUS_CANCELED:
			if got := j.eventMessage(runID, id, orkstackv1.Status_STATUS_CANCELED); got != sentence {
				t.Errorf("%s = %q", id, got)
			}
		default:
			t.Errorf("%s = %v", id, tr.GetStatus())
		}
	}
	for _, e := range j.events(runID) {
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED &&
			(e.GetTaskId() == "rollback" || e.GetTaskId() == "escalate" || e.GetTaskId() == "notify") {
			t.Errorf("a handler was scheduled in a canceled run: %v", e)
		}
	}
	proof.Write(t, "engine", "join-I6.txt", []byte(runLine(run, allTasks...)+"\n"))
}

// Operator cancel during the failing phase.
func TestIntegration_JoinCancelDuringHandlersKeepsTheRunFailed(t *testing.T) {
	j := startJoinE2E(t)
	j.orker("join-i7")
	runID := joinRunID("i7")
	j.startRun(runID, j.example(nil), map[string]any{"route": "canary", "fail_deploy": true, "smoke_seconds": 1, "rollback_seconds": 120})
	j.awaitEvent(runID, 5*time.Minute, "rollback's TASK_STARTED", taskEvent("rollback", orkstackv1.EventType_EVENT_TYPE_TASK_STARTED))
	// TASK_STARTED is the engine's word that it dispatched rollback; the
	// cancel must find the attempt running on the Orker, so the test acts
	// once the attempt heartbeats. Under a loaded proof a cancel sent on the
	// event alone can beat the Orker to the task, which then never starts
	// and leaves no attempt record.
	j.awaitLiveAttempt(runID, "rollback", 1, 3*time.Minute)
	if err := j.temporal.CancelWorkflow(j.ctx, runID, ""); err != nil {
		t.Fatal(err)
	}
	run := j.wait(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || run.GetError().GetDetails() != `task "deploy"` {
		t.Fatalf("run = %v / %v, want FAILED with deploy's error", run.GetStatus(), run.GetError())
	}
	statusIs(t, run, "rollback", orkstackv1.Status_STATUS_CANCELED)
	records := j.attemptsLanded(runID, "rollback", 1, 30*time.Second)
	if len(records) != 1 || records[0].GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_CANCELED {
		t.Errorf("rollback's attempt records = %v, want one CANCELED: a cancellation is never retried", records)
	}
	if got := j.eventMessage(runID, "escalate", orkstackv1.Status_STATUS_CANCELED); got !=
		"task canceled: the run was canceled while its failure handlers ran" {
		t.Errorf("escalate = %q", got)
	}
	if got := taskRunNamed(run, "notify").GetStatus(); got == orkstackv1.Status_STATUS_FAILED {
		t.Errorf("notify = %v", got)
	}
	proof.Write(t, "engine", "join-I7.txt", []byte(fmt.Sprintf("run=%s status=%s error=%q\n%s\n",
		runID, run.GetStatus(), run.GetError().GetMessage(), runLine(run, allTasks...))))
}

// External terminate during the failing phase.
func TestIntegration_JoinTerminatedFailingRunReadsCanceledWithItsError(t *testing.T) {
	j := startJoinE2E(t)
	j.orker("join-i8")
	runID := joinRunID("i8")
	j.startRun(runID, j.example(nil), map[string]any{"route": "canary", "fail_deploy": true, "smoke_seconds": 1, "rollback_seconds": 120})
	j.awaitEvent(runID, 5*time.Minute, "rollback's TASK_STARTED", taskEvent("rollback", orkstackv1.EventType_EVENT_TYPE_TASK_STARTED))
	reason := "operator terminated the failing run (I8)"
	if err := j.temporal.TerminateWorkflow(j.ctx, runID, "", reason); err != nil {
		t.Fatal(err)
	}
	replica := engine.NewTemporalRunReader(j.temporal).WithSnapshotStore(j.snapshots)
	read, err := replica.GetRun(j.ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if read.GetStatus() != orkstackv1.Status_STATUS_CANCELED || read.GetFinishedAt() == nil ||
		read.GetError().GetDetails() != `task "deploy"` || read.GetError().GetCode() != "RUN_FAILED" {
		t.Fatalf("terminated failing run = %v / %v", read.GetStatus(), read.GetError())
	}
	listed, err := replica.ListRuns(j.ctx, engine.RunQuery{Status: orkstackv1.Status_STATUS_CANCELED})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, r := range listed {
		found = found || r.GetId() == runID
	}
	if !found {
		t.Errorf("ListRuns(CANCELED) does not list %s", runID)
	}
	events, err := replica.Events(j.ctx, runID, 0)
	if err != nil {
		t.Fatal(err)
	}
	last := events[len(events)-1]
	if last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED || !strings.Contains(last.GetMessage(), reason) {
		t.Fatalf("last event = %v %q", last.GetType(), last.GetMessage())
	}
	proof.Write(t, "engine", "join-I8.txt", []byte(fmt.Sprintf("read status=%s error=%q\nlast event=%s %q\n",
		read.GetStatus(), read.GetError().GetMessage(), last.GetType(), last.GetMessage())))
}

// awaitLiveAttempt blocks until rollback's attempt `attempt` is started with a
// heartbeat naming its orker_id, and returns that Orker's id.
func (j *joinE2E) awaitLiveAttempt(runID, taskID string, attempt int32, within time.Duration) string {
	j.t.Helper()
	reader := engine.NewTemporalRunReader(j.temporal)
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		live, err := reader.LiveAttempts(j.ctx, runID, time.Now())
		if err == nil {
			for _, a := range live.Attempts {
				if a.GetTaskId() == taskID && a.GetAttempt() == attempt && a.GetHeartbeatAvailable() && a.GetOrkerId() != "" {
					return a.GetOrkerId()
				}
			}
		}
		select {
		case <-j.ctx.Done():
			j.t.Fatal("context ended")
		case <-time.After(200 * time.Millisecond):
		}
	}
	j.t.Fatalf("%s attempt %d never reported a heartbeat", taskID, attempt)
	return ""
}

// killRollbackSeconds is the rollback_seconds the killUnderRollback runs
// hold rollback open with. Attempt 1 has to still be running when the kill
// lands: awaitLiveAttempt sees its first heartbeat within one heartbeat tick
// (5 s by default) of the attempt's start, and 30 s leaves a wide margin. A
// rollback that ended first would SUCCEED on attempt 1, which both tests'
// assertions refuse explicitly. A killed container that then exits 0 inside
// its SIGTERM grace is still recorded CANCELED (the Orker checks
// cancellation first). The retry re-runs the whole hold after the 30 s
// heartbeat timeout, so this value is paid twice; do not lengthen it again
// without a reason.
const killRollbackSeconds = 30

// killUnderRollback is the kill-under-a-handler procedure: two Orkers; once notify
// succeeded and rollback's first attempt heartbeats, stop the Orker under it.
func (j *joinE2E) killUnderRollback(runID string, workers map[string]worker.Worker) string {
	j.t.Helper()
	j.awaitEvent(runID, 5*time.Minute, "notify's TASK_SUCCEEDED", taskEvent("notify", orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED))
	victim := j.awaitLiveAttempt(runID, "rollback", 1, 3*time.Minute)
	j.kill(victim, workers[victim])
	return victim
}

// heartbeatRetry returns taskID's first retried ActivityTaskStarted: its
// attempt and the timeout type of its last_failure. Only that task's
// ExecuteTask counts — deploy, which fails twice in these runs, has a
// retried ActivityTaskStarted of its own (an ActivityFailed, no timeout).
func heartbeatRetry(t *testing.T, h *history.History, taskID string) (int32, string) {
	t.Helper()
	scheduled := map[int64]bool{}
	for _, event := range h.GetEvents() {
		if attrs := event.GetActivityTaskScheduledEventAttributes(); attrs != nil {
			if attrs.GetActivityType().GetName() == task.ActivityExecuteTask {
				scheduled[event.GetEventId()] = executeTaskIDFrom(t, attrs.GetInput()) == taskID
			}
			continue
		}
		attrs := event.GetActivityTaskStartedEventAttributes()
		if attrs == nil || attrs.GetAttempt() < 2 || !scheduled[attrs.GetScheduledEventId()] {
			continue
		}
		// The proto name (TIMEOUT_TYPE_HEARTBEAT), not String()'s "Heartbeat".
		return attrs.GetAttempt(), enums.TimeoutType_name[int32(attrs.GetLastFailure().GetTimeoutFailureInfo().GetTimeoutType())]
	}
	return 0, ""
}

// An Orker killed under a running handler retries it per its policy.
func TestIntegration_JoinOrkerKilledUnderAHandlerRetriesIt(t *testing.T) {
	j := startJoinE2E(t)
	workers := map[string]worker.Worker{"join-i9-a": j.orker("join-i9-a"), "join-i9-b": j.orker("join-i9-b")}
	runID := joinRunID("i9")
	j.startRun(runID, j.example(nil), map[string]any{"route": "canary", "fail_deploy": true, "smoke_seconds": 1, "rollback_seconds": killRollbackSeconds})
	victim := j.killUnderRollback(runID, workers)
	run := j.wait(runID)
	rollback := taskRunNamed(run, "rollback")
	if rollback.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || rollback.GetOrkerId() == victim {
		t.Fatalf("rollback = %v on %q (victim %q)", rollback.GetStatus(), rollback.GetOrkerId(), victim)
	}
	attempt, timeoutType := heartbeatRetry(t, j.history(runID), "rollback")
	if attempt != 2 || timeoutType != enums.TimeoutType_name[int32(enums.TIMEOUT_TYPE_HEARTBEAT)] {
		t.Fatalf("history: attempt %d after %s, want attempt 2 after a HEARTBEAT timeout", attempt, timeoutType)
	}
	// Attempt 1 reported nothing to the cluster (run attempts reads it
	// UNREPORTED); at most a CANCELED record of its own stopped container.
	// Attempt 2 SUCCEEDED on the survivor.
	records := j.attemptsOf(runID, "rollback")
	second := false
	for _, r := range records {
		switch {
		case r.GetAttempt() == 2 && r.GetOutcome() == orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED:
			second = r.GetOrkerId() != victim
		case r.GetAttempt() == 1 && r.GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_CANCELED:
			t.Errorf("rollback attempt 1 recorded %v; the killed Orker reported nothing", r.GetOutcome())
		}
	}
	if !second {
		t.Fatalf("rollback's records = %v: want attempt 2 SUCCEEDED on the survivor", records)
	}
	statusIs(t, run, "escalate", orkstackv1.Status_STATUS_SKIPPED)
	statusIs(t, run, "notify", orkstackv1.Status_STATUS_SUCCEEDED)
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || run.GetError().GetDetails() != `task "deploy"` {
		t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
	}
	proof.Write(t, "engine", "join-I9.txt", []byte(fmt.Sprintf(
		"victim=%s survivor=%s\nhistory: ActivityTaskStarted attempt=%d last_failure=%s\nrollback records: %v\n%s\n",
		victim, rollback.GetOrkerId(), attempt, timeoutType, records, runLine(run, allTasks...))))
}

// The same kill, with no retry policy.
func TestIntegration_JoinOrkerKilledUnderAHandlerWithoutRetryFailsIt(t *testing.T) {
	j := startJoinE2E(t)
	workers := map[string]worker.Worker{"join-i9b-a": j.orker("join-i9b-a"), "join-i9b-b": j.orker("join-i9b-b")}
	runID := joinRunID("i9b")
	j.startRun(runID, j.example(noRetryVariant), map[string]any{"route": "canary", "fail_deploy": true, "smoke_seconds": 1, "rollback_seconds": killRollbackSeconds})
	j.killUnderRollback(runID, workers)
	run := j.wait(runID)
	rollback := taskRunNamed(run, "rollback")
	if rollback.GetStatus() != orkstackv1.Status_STATUS_FAILED || rollback.GetError().GetCode() != "ORKER_HEARTBEAT_TIMEOUT" ||
		rollback.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR ||
		rollback.GetRetryState() != orkstackv1.RetryState_RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED {
		t.Fatalf("rollback = %v", rollback)
	}
	for _, r := range j.attemptsOf(runID, "rollback") {
		if r.GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_CANCELED {
			t.Errorf("the killed attempt recorded %v; it reported nothing", r.GetOutcome())
		}
	}
	if !strings.Contains(j.attemptLog(runID, "rollback", 1), "rolling back") {
		t.Errorf("the log store holds no line of rollback's attempt 1")
	}
	statusIs(t, run, "escalate", orkstackv1.Status_STATUS_SUCCEEDED)
	statusIs(t, run, "notify", orkstackv1.Status_STATUS_SUCCEEDED)
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || run.GetError().GetDetails() != `task "deploy"` {
		t.Fatalf("run = %v / %v, want deploy's error, not rollback's", run.GetStatus(), run.GetError())
	}
	proof.Write(t, "engine", "join-I9b.txt", []byte(fmt.Sprintf("rollback code=%s category=%s retry_state=%s\nrun error=%q\n",
		rollback.GetError().GetCode(), rollback.GetError().GetCategory(), rollback.GetRetryState(), run.GetError().GetMessage())))
}

// Engine restarted during the failing phase.
func TestIntegration_JoinEngineRestartDuringHandlersDispatchesNothingTwice(t *testing.T) {
	j := startJoinE2E(t)
	j.orker("join-i10")
	runID := joinRunID("i10")
	j.startRun(runID, j.example(nil), map[string]any{"route": "canary", "fail_deploy": true, "smoke_seconds": 1, "rollback_seconds": 30})
	j.awaitEvent(runID, 5*time.Minute, "rollback's TASK_STARTED", taskEvent("rollback", orkstackv1.EventType_EVENT_TYPE_TASK_STARTED))
	j.mu.Lock()
	old := j.runWorker
	j.mu.Unlock()
	old.Stop()
	j.startRunWorker()
	run := j.wait(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Fatalf("run = %v", run.GetStatus())
	}
	var lines []string
	for _, id := range []string{"decide", "canary", "verify", "deploy", "smoke", "rollback", "notify"} {
		n := j.invocations(runID, id)
		lines = append(lines, fmt.Sprintf("%s=%d", id, n))
		want := 1
		if id == "deploy" {
			want = 2
		}
		if n != want {
			t.Errorf("%s invoked %d times, want %d", id, n, want)
		}
	}
	if n := len(j.attemptsOf(runID, "rollback")); n != 1 {
		t.Errorf("rollback has %d attempt records, want 1", n)
	}
	proof.Write(t, "engine", "join-I10.txt", []byte(strings.Join(lines, " ")+"\n"))
}

// Composed.
func TestIntegration_JoinComposedHandlersRunInTheirOrder(t *testing.T) {
	j := startJoinE2E(t)
	j.orker("join-i11")
	child := j.compileDoc(`apiVersion: orkstack/v1
kind: Orkflow
name: join-child
version: "1.0.0"
tasks:
  - id: work
    uses: shell
    with: {op: exec, command: ["sh", "-c", "echo child failing; exit 1"]}
  - id: tidy
    uses: shell
    needs: [work]
    join: any_failed
    with: {op: exec, command: ["echo", "child tidied"]}
`)
	block := j.compileDoc(`apiVersion: orkstack/v1
kind: Orkflow
name: rollback-block
version: "1.0.0"
tasks:
  - id: undo
    uses: shell
    with: {op: exec, command: ["echo", "undone"]}
`)
	parent := j.compileDoc(`apiVersion: orkstack/v1
kind: Orkflow
name: join-parent
version: "1.0.0"
tasks:
  - id: release
    uses: orkflow:join-child@1.0.0
  - id: report
    uses: shell
    needs: [release]
    join: any_failed
    with: {op: exec, command: ["echo", "the child failed"]}
  - id: compensate
    uses: orkflow:rollback-block@1.0.0
    needs: [release]
    join: any_failed
`)
	for _, tk := range parent.GetTasks() {
		switch tk.GetOrkflow().GetName() {
		case "join-child":
			tk.Orkflow.DefinitionDigest = child.GetDefinitionDigest()
		case "rollback-block":
			tk.Orkflow.DefinitionDigest = block.GetDefinitionDigest()
		}
	}
	plan := j.cachePlan(false)
	plan.Children = map[string]*engine.ChildRunPlan{}
	for id, wf := range map[string]*orkstackv1.Orkflow{"release": child, "compensate": block} {
		cp, err := engine.NewChildRunPlan(wf, j.cachePlan(false), false, false)
		if err != nil {
			t.Fatal(err)
		}
		cp.RunID = joinRunID("i11-" + id)
		plan.Children[id] = cp
	}
	runID := joinRunID("i11")
	j.startWith(runID, parent, map[string]any{}, plan)
	run := j.wait(runID)
	childRun := j.wait(plan.Children["release"].RunID)
	statusIs(t, childRun, "tidy", orkstackv1.Status_STATUS_SUCCEEDED)
	statusIs(t, run, "report", orkstackv1.Status_STATUS_SUCCEEDED)
	statusIs(t, run, "compensate", orkstackv1.Status_STATUS_SUCCEEDED)
	compensated := j.wait(plan.Children["compensate"].RunID)
	if compensated.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("rollback-block child = %v", compensated.GetStatus())
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || taskRunNamed(run, "release").GetError().GetCode() != engine.ChildRunFailedCode ||
		taskRunNamed(run, "release").GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED {
		t.Fatalf("parent = %v, release = %v", run.GetStatus(), taskRunNamed(run, "release").GetError())
	}
	if !taskRunNamed(childRun, "tidy").GetFinishedAt().AsTime().Before(taskRunNamed(run, "report").GetStartedAt().AsTime()) {
		t.Errorf("the parent's handler started before the child's own handler finished")
	}
	proof.Write(t, "engine", "join-I11.txt", []byte(runLine(run, "release", "report", "compensate")+"\n"))
}

func signalAwait(t *testing.T, j *joinE2E, runID string, sig *orkstackv1.AwaitSignal) {
	t.Helper()
	if err := j.temporal.SignalWorkflow(j.ctx, runID, "", engine.ResolveAwaitSignal, sig); err != nil {
		t.Fatalf("signal %s: %v", runID, err)
	}
}

// Awaited.
func TestIntegration_JoinAwaitedHandlers(t *testing.T) {
	j := startJoinE2E(t)
	j.orker("join-i12")
	// (a) A rejected approval gate fires its any_failed handler.
	gate := j.compileDoc(`apiVersion: orkstack/v1
kind: Orkflow
name: join-gate
version: "1.0.0"
tasks:
  - id: gate
    await: {timeout_seconds: 600}
  - id: ship
    uses: shell
    needs: [gate]
    with: {op: exec, command: ["echo", "shipping"]}
  - id: tidy
    uses: shell
    needs: [gate]
    join: any_failed
    with: {op: exec, command: ["echo", "tidied"]}
`)
	runA := joinRunID("i12a")
	j.startRun(runA, gate, map[string]any{})
	j.awaitEvent(runA, 2*time.Minute, "gate's TASK_AWAITING", taskEvent("gate", orkstackv1.EventType_EVENT_TYPE_TASK_AWAITING))
	signalAwait(t, j, runA, &orkstackv1.AwaitSignal{TaskId: "gate", Reject: true, Reason: "not today",
		Via: orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API})
	run := j.wait(runA)
	statusIs(t, run, "tidy", orkstackv1.Status_STATUS_SUCCEEDED)

	// (b) The await variant: rollback opens after the failing note.
	wf := j.example(awaitVariant)
	for i, reject := range []bool{false, true} {
		runID := joinRunID(fmt.Sprintf("i12b-%d", i))
		j.startRun(runID, wf, map[string]any{"route": "canary", "fail_deploy": true, "smoke_seconds": 1})
		awaiting := j.awaitEvent(runID, 5*time.Minute, "rollback's TASK_AWAITING", taskEvent("rollback", orkstackv1.EventType_EVENT_TYPE_TASK_AWAITING))
		notes := failingNotes(j.events(runID))
		if len(notes) != 1 || notes[0].GetSequence() > awaiting.GetSequence() {
			t.Fatalf("rollback opened at #%d, failing notes %v", awaiting.GetSequence(), notes)
		}
		sig := &orkstackv1.AwaitSignal{TaskId: "rollback", Via: orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API}
		if reject {
			sig.Reject, sig.Reason = true, "rollback refused"
		} else {
			body := []byte(`{"approved":true}`)
			sig.ValuesJson = body
			sig.Values = &orkstackv1.ArtifactRef{Name: "values.json", Uri: "mem://approved", SizeBytes: int64(len(body))}
		}
		signalAwait(t, j, runID, sig)
		run := j.wait(runID)
		if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || run.GetError().GetDetails() != `task "deploy"` {
			t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
		}
		if reject {
			statusIs(t, run, "rollback", orkstackv1.Status_STATUS_FAILED)
			statusIs(t, run, "escalate", orkstackv1.Status_STATUS_SUCCEEDED)
		} else {
			statusIs(t, run, "rollback", orkstackv1.Status_STATUS_SUCCEEDED)
			statusIs(t, run, "escalate", orkstackv1.Status_STATUS_SKIPPED)
		}
	}
	proof.Write(t, "engine", "join-I12.txt", []byte("rejected gate fired tidy; await variant resolved and rejected while failing\n"))
}

// Fan-out.
func TestIntegration_JoinAnItemFailureStartsTheFanOutsHandler(t *testing.T) {
	j := startJoinE2E(t)
	j.orker("join-i13")
	wf := j.compileDoc(`apiVersion: orkstack/v1
kind: Orkflow
name: join-items
version: "1.0.0"
inputs:
  regions: {type: array}
tasks:
  - id: convert
    uses: shell
    each: {over: "{{ .inputs.regions }}", max_items: 5, max_parallel: 1}
    with:
      op: exec
      command: ["sh", "-c", "if [ \"$1\" = us ]; then exit 1; fi", "item", "{{ .each.item }}"]
  - id: report
    uses: shell
    needs: [convert]
    join: any_failed
    with:
      op: script
      interpreter: sh
      script: |
        jq -c '.join' /orkstack/input/task.json
        jq -c '.inputs.artifacts' /orkstack/input/task.json
`)
	runID := joinRunID("i13")
	j.startRun(runID, wf, map[string]any{"regions": []any{"eu", "us", "ap"}})
	run := j.wait(runID)
	node := taskRunNamed(run, "convert")
	if node.GetStatus() != orkstackv1.Status_STATUS_FAILED || !strings.HasPrefix(node.GetError().GetMessage(), "item 1 of 3 failed") {
		t.Fatalf("convert = %v", node.GetError())
	}
	statusIs(t, run, "report", orkstackv1.Status_STATUS_SUCCEEDED)
	out := j.attemptLog(runID, "report", 1)
	if !strings.Contains(out, "item 1 of 3 failed") || !strings.Contains(out, "\n[]") {
		t.Fatalf("report's stdout:\n%s", out)
	}
	proof.Write(t, "engine", "join-I13.txt", []byte(out))
}

// A lost pin: three pin hosts; the host running the pinned member is
// killed; the unpinned any_failed handler runs on a survivor.
func TestIntegration_JoinALostPinStartsAnUnpinnedHandler(t *testing.T) {
	j := startJoinE2E(t)
	refs, _ := j.store.(artifacts.RefPredictor)
	hosts := map[string]*pinOrker{}
	for _, id := range []string{"join-i14-a", "join-i14-b", "join-i14-c"} {
		hosts[id] = newPinOrker(t, j.ctx, pinOrkerConfig{
			id: id, pool: task.DefaultPool, queue: j.orkerQueue, store: j.store, commits: j.commits,
			logs: j.logs, refs: refs, temporalNamespace: j.namespace,
		})
	}
	wf := j.compileDoc(`apiVersion: orkstack/v1
kind: Orkflow
name: join-pin
version: "1.0.0"
pins:
  build: {}
tasks:
  - id: compile
    uses: shell
    pin: build
    with: {op: exec, command: ["sh", "-c", "echo compiling; sleep 120"]}
  - id: package
    uses: shell
    pin: build
    needs: [compile]
    with: {op: exec, command: ["echo", "packaged"]}
  - id: recover
    uses: shell
    needs: [compile]
    join: any_failed
    with: {op: exec, command: ["echo", "recovered"]}
`)
	runID := joinRunID("i14")
	j.startRun(runID, wf, map[string]any{})
	j.awaitEvent(runID, 5*time.Minute, "compile's TASK_STARTED", taskEvent("compile", orkstackv1.EventType_EVENT_TYPE_TASK_STARTED))
	victim := ""
	deadline := time.Now().Add(3 * time.Minute)
	for victim == "" && time.Now().Before(deadline) {
		for id, h := range hosts {
			if h.sawTask(runID, "compile") {
				victim = id
			}
		}
		if victim == "" {
			select {
			case <-j.ctx.Done():
				t.Fatal("context ended")
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
	if victim == "" {
		t.Fatal("no pin host ran compile")
	}
	hosts[victim].kill()
	run := j.wait(runID)
	compile := taskRunNamed(run, "compile")
	if compile.GetStatus() != orkstackv1.Status_STATUS_FAILED || compile.GetError().GetCode() != "PIN_HOST_LOST" {
		t.Fatalf("compile = %v", compile)
	}
	recover := taskRunNamed(run, "recover")
	if recover.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || recover.GetOrkerId() == victim {
		t.Fatalf("recover = %v on %q", recover.GetStatus(), recover.GetOrkerId())
	}
	for id, h := range hosts {
		if id != victim && h.sawTask(runID, "package") {
			t.Errorf("survivor %s was handed the lost pin's package", id)
		}
	}
	proof.Write(t, "engine", "join-I14.txt", []byte(fmt.Sprintf("victim=%s compile=%s recover on %s\n",
		victim, compile.GetError().GetCode(), recover.GetOrkerId())))
}

// Cache on a re-join.
func TestIntegration_JoinCachedReJoinHitsForTheSameRoute(t *testing.T) {
	j := startJoinE2E(t)
	j.orker("join-i15")
	wf := j.compileDoc(`apiVersion: orkstack/v1
kind: Orkflow
name: join-cache
version: "1.0.0"
inputs:
  route: {type: string}
tasks:
  - id: canary
    uses: shell
    condition: '{{ eq .inputs.route "canary" }}'
    with: {op: exec, command: ["echo", "canary"]}
  - id: full
    uses: shell
    condition: '{{ eq .inputs.route "full" }}'
    with: {op: exec, command: ["echo", "full"]}
  - id: verify
    uses: shell
    needs: [canary, full]
    join: none_failed
    cache: {enabled: true}
    with: {op: exec, command: ["echo", "verified"]}
`)
	outcome := func(route string) orkstackv1.CacheOutcome {
		runID := joinRunID("i15-" + route)
		j.startRun(runID, wf, map[string]any{"route": route})
		run := j.wait(runID)
		if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
		}
		return taskRunNamed(run, "verify").GetCache().GetOutcome()
	}
	first, second, other := outcome("canary"), outcome("canary"), outcome("full")
	if first != orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS || second != orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT ||
		other != orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS {
		t.Fatalf("verify cache outcomes = %v, %v, %v; want MISS, HIT, MISS", first, second, other)
	}
	proof.Write(t, "engine", "join-I15.txt", []byte(fmt.Sprintf("canary=%s canary=%s full=%s\n", first, second, other)))
}

// A broken condition.
func TestIntegration_JoinBrokenConditionFailsItsTaskWhenAHandlerExists(t *testing.T) {
	j := startJoinE2E(t)
	j.orker("join-i16")
	base := `apiVersion: orkstack/v1
kind: Orkflow
name: join-broken
version: "1.0.0"
inputs:
  a: {type: string, default: x}
  b: {type: number, default: 1}
tasks:
  - id: first
    uses: shell
    with: {op: exec, command: ["echo", "first"]}
  - id: broken
    uses: shell
    needs: [first]
    condition: "{{ eq .inputs.a .inputs.b }}"
    with: {op: exec, command: ["echo", "never"]}
`
	handler := `  - id: rollback
    uses: shell
    needs: [broken]
    join: any_failed
    with: {op: exec, command: ["echo", "rolled back"]}
`
	runID := joinRunID("i16")
	j.startRun(runID, j.compileDoc(base+handler), map[string]any{})
	run := j.wait(runID)
	broken := taskRunNamed(run, "broken")
	if broken.GetStatus() != orkstackv1.Status_STATUS_FAILED || broken.GetError().GetCode() != "CONDITION_FAILED" {
		t.Fatalf("broken = %v", broken)
	}
	statusIs(t, run, "rollback", orkstackv1.Status_STATUS_SUCCEEDED)
	if !strings.Contains(run.GetError().GetMessage(), "incompatible types") || engine.PrimaryFailedTask(run).GetTaskId() != "broken" {
		t.Fatalf("run error = %v, primary %v", run.GetError(), engine.PrimaryFailedTask(run))
	}
	plainID := joinRunID("i16-plain")
	j.startRun(plainID, j.compileDoc(base), map[string]any{})
	plain := j.wait(plainID)
	statusIs(t, plain, "broken", orkstackv1.Status_STATUS_CANCELED)
	proof.Write(t, "engine", "join-I16.txt", []byte(fmt.Sprintf("with handler: broken=%s/%s\nwithout: broken=%s\n",
		broken.GetStatus(), broken.GetError().GetCode(), taskRunNamed(plain, "broken").GetStatus())))
}

// An Orker that predates joins.
func TestIntegration_JoinSubmitRefusesAPoolWhoseOrkerPredatesJoins(t *testing.T) {
	j := startJoinE2E(t)
	pool := fmt.Sprintf("old-%d", time.Now().UnixNano()%1_000_000)
	queue, err := task.QueueForPool(pool, "")
	if err != nil {
		t.Fatal(err)
	}
	w := worker.New(j.temporal, queue, worker.Options{DisableRegistrationAliasing: true, DisableWorkflowWorker: true})
	w.RegisterActivityWithOptions(func(context.Context) (*orkstackv1.DescribeOrkerResult, error) {
		return &orkstackv1.DescribeOrkerResult{OrkerId: "old-orker", BuildId: "2026.09.01", Pool: pool,
			EachSupported: true, CacheSupported: true}, nil
	}, activity.RegisterOptions{Name: task.ActivityDescribeOrker})
	w.RegisterActivityWithOptions(func(context.Context, *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return nil, temporal.NewNonRetryableApplicationError("never", "ActivityFailed", nil)
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)
	starter := engine.NewTemporalStarterWithOrkerQueue(j.temporal, j.runQueue, j.orkerQueue)
	resolver := &engine.PoolResolver{Probe: engine.NewTemporalPoolProbe(j.temporal), Orkers: starter}
	ns := &orkstackv1.Namespace{Name: "default", DefaultPool: pool}
	wf := j.example(nil)
	deadline := time.Now().Add(time.Minute)
	for {
		var refused *engine.PoolNoJoinSupportError
		_, _, err := resolver.Plan(j.ctx, ns, wf, true)
		if errors.As(err, &refused) {
			if strings.Join(refused.Tasks, ", ") != "rollback, escalate, notify" || refused.OrkerID != "old-orker" {
				t.Fatalf("refusal = %+v", refused)
			}
			// Without its failure handlers the same definition is accepted.
			var kept []*orkstackv1.Task
			for _, tk := range wf.GetTasks() {
				if !dsl.IsFailureHandler(tk) {
					kept = append(kept, tk)
				}
			}
			wf.Tasks = kept
			if _, _, err := resolver.Plan(j.ctx, ns, wf, false); err != nil {
				t.Fatalf("the definition without handlers = %v", err)
			}
			proof.Write(t, "engine", "join-I18.txt", []byte("FailedPrecondition: "+refused.Error()+"\n"))
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Plan = %v, want PoolNoJoinSupportError", err)
		}
		select {
		case <-j.ctx.Done():
			t.Fatalf("context ended: %v", err)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// Opt-in recorder of testdata/history/run-join-v1.json: the shipped
// example with route=full, fail_deploy=true and smoke_seconds=120, on a
// VERSIONED worker and a real Orker.
func TestIntegration_RecordJoinRunHistory(t *testing.T) {
	if os.Getenv("ORK_RECORD_GOLDEN_HISTORY") == "" {
		t.Skip("set ORK_RECORD_GOLDEN_HISTORY=1 to record " + joinHistory +
			" — the committed fixture is meant to age, not to be refreshed")
	}
	j := startJoinE2E(t)
	namespace := versioningNamespace()
	temporalClient := dialVersioningCluster(t, j.ctx, namespace)
	suffix := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	runQueue := "ork-join-rec-run-" + suffix
	orkerQueue := "ork-join-rec-tasks-" + suffix
	deployment := task.Deployment{Name: "ork-join-rec-" + suffix, BuildID: "v1"}

	runWorker := worker.New(temporalClient, runQueue, deployment.Apply(worker.Options{
		DisableRegistrationAliasing: true, LocalActivityWorkerOnly: true,
	}))
	engine.RegisterWorkflows(runWorker)
	runWorker.RegisterActivityWithOptions(engine.NewSnapshotActivities(j.snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	refs, _ := j.store.(artifacts.RefPredictor)
	orkerActivities := newRecorderOrker(t, j, refs)
	orkerWorker := worker.New(temporalClient, orkerQueue, deployment.Apply(worker.Options{
		DisableRegistrationAliasing: true, DisableWorkflowWorker: true,
	}))
	orkerWorker.RegisterActivityWithOptions(orkerActivities.ExecuteTask, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	eventWorker := worker.New(temporalClient, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true, DisableWorkflowWorker: true,
	})
	eventWorker.RegisterActivityWithOptions(
		engine.NewEventActivities(engine.NewEventBus(), engine.NewRunStore()).WithSnapshotStore(j.snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	for name, w := range map[string]worker.Worker{"run": runWorker, "orker": orkerWorker, "event": eventWorker} {
		if err := w.Start(); err != nil {
			t.Fatalf("start %s worker: %v", name, err)
		}
		defer w.Stop()
	}
	setCurrentVersion(t, j.ctx, temporalClient, deployment, runQueue, orkerQueue)
	starter := engine.NewTemporalStarterWithOrkerQueue(temporalClient, runQueue, orkerQueue)
	plan := rawPlan(orkerQueue)
	root, err := artifacts.CommitRoot(j.cfg)
	if err != nil {
		t.Fatal(err)
	}
	plan.CommitRoot = root
	plan.Limits = engine.DefaultLimits()
	plan.TransferBudgetSeconds = 300
	input, _ := structpb.NewStruct(map[string]any{"route": "full", "fail_deploy": true, "smoke_seconds": 120})
	runID := fmt.Sprintf("ork-join-fixture-%d", time.Now().UnixNano())
	if err := starter.StartRun(j.ctx, runID, j.example(nil), &orkstackv1.Payload{Data: input}, plan); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if err := temporalClient.GetWorkflow(j.ctx, runID, "").Get(j.ctx, nil); err == nil {
		t.Fatalf("fixture run %s succeeded; the join fixture is a failing run", runID)
	}
	recorded := &history.History{}
	iter := temporalClient.GetWorkflowHistory(j.ctx, runID, "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			t.Fatalf("read history: %v", err)
		}
		recorded.Events = append(recorded.Events, event)
	}
	encoded, err := temporalproto.CustomJSONMarshalOptions{Indent: "  "}.Marshal(recorded)
	if err != nil {
		t.Fatalf("encode history: %v", err)
	}
	if err := os.WriteFile(joinHistory, append(encoded, '\n'), 0o644); err != nil {
		t.Fatalf("write %s: %v", joinHistory, err)
	}
	t.Logf("recorded %d history events for run %s into %s (deployment version %s)",
		len(recorded.Events), runID, joinHistory, deployment)
}

// newRecorderOrker is the fixture recorder's Orker, on this suite's stores.
func newRecorderOrker(t *testing.T, j *joinE2E, refs artifacts.RefPredictor) *orker.Activities {
	t.Helper()
	activities := orker.NewActivitiesWithOptions(j.store, orker.Options{
		Logs: j.logs, Commits: j.commits, Attempts: j.attempts, Refs: refs, Memos: j.memos,
		Pool: task.DefaultPool, OrkerID: proof.OrkerID("join-fixture"), WorkRoot: j.workRoot,
	})
	if err := activities.Boot(j.ctx); err != nil {
		infraAbsent(t, "orker boot checks failed: %v", err)
	}
	return activities
}
