//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// sleepIntegrationYAML is a container, a short sleep, and a container after
// it, as an author writes it.
const sleepIntegrationYAML = `
apiVersion: orkstack/v1
kind: Orkflow
name: sleep-integration
version: "1.0.0"
tasks:
  - id: deploy
    uses: shell
    with: {op: exec, command: ["echo", "deploy"]}
  - id: soak
    needs: [deploy]
    sleep: {seconds: 3}
  - id: promote
    uses: shell
    needs: [soak]
    with: {op: exec, command: ["echo", "promote"]}
`

// sleepOnlyOrkflow is one sleep of seconds and nothing else: it needs no
// Orker and no route.
func sleepOnlyOrkflow(name string, seconds int32) *orkstackv1.Orkflow {
	return &orkstackv1.Orkflow{Name: name, Version: "1.0.0", Tasks: []*orkstackv1.Task{
		{Id: "nap", Sleep: &orkstackv1.SleepSpec{Seconds: seconds}},
		{Id: "after", Needs: []string{"nap"}, Sleep: &orkstackv1.SleepSpec{Seconds: 1}},
	}}
}

// timerFired reports whether runID's current execution has recorded a fired
// timer: the cluster's own record that a sleep's time passed, whether or not
// any worker is polling.
func timerFired(t *testing.T, s *awaitStack, runID string) bool {
	t.Helper()
	iter := s.client.GetWorkflowHistory(s.ctx, runID, "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		must.OKf(t, err, "read history of %s", runID)
		if event.GetEventType() == enums.EVENT_TYPE_TIMER_FIRED {
			return true
		}
	}
	return false
}

// A sleep starts no activity, is SLEEPING with its wake-up time while it
// sleeps, then SUCCEEDED; the task after it runs only once it woke, and is
// handed no reference for it.
func TestIntegration_SleepWakesAndItsDependantRuns(t *testing.T) {
	s := newAwaitStack(t, awaitStackOptions{})
	s.apply(sleepIntegrationYAML)
	runID := s.submit("sleep-integration").GetRun().GetId()

	sleeping := s.waitTask(runID, "soak", orkstackv1.Status_STATUS_SLEEPING)
	soak := taskRunNamed(sleeping, "soak")
	wakeAt := soak.GetSleep().GetWakeAt().AsTime()
	if soak.GetSleep().GetSeconds() != 3 || !wakeAt.Equal(soak.GetStartedAt().AsTime().Add(3*time.Second)) {
		t.Fatalf("soak while sleeping = %v, want 3 s and wake_at = started_at + 3 s", soak)
	}
	if got := taskRunNamed(sleeping, "promote").GetStatus(); got != orkstackv1.Status_STATUS_PENDING {
		t.Fatalf("promote is %v while soak sleeps, want PENDING", got)
	}

	run := s.waitRun(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v (%s)", run.GetStatus(), run.GetError().GetMessage())
	}
	soak = taskRunNamed(run, "soak")
	if soak.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || soak.GetFinishedAt().AsTime().Before(wakeAt) {
		t.Fatalf("soak = %v, want SUCCEEDED no earlier than %v", soak, wakeAt)
	}
	if soak.GetValuesRef() != nil || soak.GetCommit() != nil || soak.GetPool() != "" || soak.GetOrkerId() != "" {
		t.Fatalf("a sleep carries values, a commit or container fields: %v", soak)
	}
	events := eventsOfRun(t, s, runID)
	if n := countEvents(events, "soak", orkstackv1.EventType_EVENT_TYPE_TASK_SLEEPING); n != 1 {
		t.Fatalf("%d TASK_SLEEPING events, want 1", n)
	}
	if n := countEvents(events, "soak", orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED); n != 0 {
		t.Fatalf("soak was scheduled as an activity %d times", n)
	}
	for _, e := range events {
		if e.GetTaskId() == "promote" && e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED &&
			e.GetTimestamp().AsTime().Before(wakeAt) {
			t.Fatalf("promote was scheduled at %v, before soak woke at %v", e.GetTimestamp().AsTime(), wakeAt)
		}
	}
	s.mu.Lock()
	in := s.inputs[runID+"/promote"]
	_, soakRan := s.inputs[runID+"/soak"]
	s.mu.Unlock()
	if in == nil || soakRan {
		t.Fatalf("promote dispatched: %v; soak dispatched as an activity: %v", in != nil, soakRan)
	}
	for _, ref := range in.GetUpstream() {
		if ref.GetTaskId() == "soak" {
			t.Fatalf("promote was handed a reference for the sleep: %v", ref)
		}
	}
	writeAwaitProof(t, "sleep-I1.txt", fmt.Sprintf("run %s: soak SLEEPING until %s, woke at %s, promote ran after it\n",
		runID, wakeAt.Format(time.RFC3339Nano), soak.GetFinishedAt().AsTime().Format(time.RFC3339Nano)))
}

// An operator's cancel ends a sleeping run CANCELED at once, with the sleep
// CANCELED before its wake-up time.
func TestIntegration_RunCanceledWhileSleepingEndsCanceled(t *testing.T) {
	s := newAwaitStack(t, awaitStackOptions{noOrker: true})
	runID := engine.NewRunID()
	must.OKf(t, s.starter.StartRun(s.ctx, runID, sleepOnlyOrkflow("sleep-cancel", 3600), nil,
		engine.RunPlan{Namespace: "default", Routes: map[string]string{}}), "StartRun")
	s.waitTask(runID, "nap", orkstackv1.Status_STATUS_SLEEPING)
	must.OKf(t, s.starter.CancelRun(s.ctx, runID, engine.CancelRequest{Reason: "operator"}), "CancelRun")
	run := s.waitRun(runID)
	nap := taskRunNamed(run, "nap")
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED || nap.GetStatus() != orkstackv1.Status_STATUS_CANCELED ||
		nap.GetFinishedAt() == nil || !nap.GetFinishedAt().AsTime().Before(nap.GetSleep().GetWakeAt().AsTime()) {
		t.Fatalf("run %v, nap %v", run.GetStatus(), nap)
	}
	if got := taskRunNamed(run, "after").GetStatus(); got != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("after = %v, want CANCELED", got)
	}
	events := eventsOfRun(t, s, runID)
	if n := countEvents(events, "nap", orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED); n != 0 {
		t.Fatalf("a canceled sleep succeeded %d times", n)
	}
}

// AGENTS.md §15 "an accepted execution cannot disappear": the run worker is
// gone when the sleep's time passes; the cluster records the timer firing,
// and a fresh worker replays the run and carries it on.
func TestIntegration_SleepThatWakesWithNoEngineWorkerCarriesOnAfterRestart(t *testing.T) {
	s := newAwaitStack(t, awaitStackOptions{noOrker: true})
	runID := engine.NewRunID()
	must.OKf(t, s.starter.StartRun(s.ctx, runID, sleepOnlyOrkflow("sleep-restart", 3), nil,
		engine.RunPlan{Namespace: "default", Routes: map[string]string{}}), "StartRun")
	sleeping := s.waitTask(runID, "nap", orkstackv1.Status_STATUS_SLEEPING)
	wakeAt := taskRunNamed(sleeping, "nap").GetSleep().GetWakeAt().AsTime()

	s.stopRunWorker()
	deadline := time.Now().Add(60 * time.Second)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for !timerFired(t, s, runID) {
		if time.Now().After(deadline) {
			t.Fatalf("the cluster never recorded nap's timer firing (wake_at %v)", wakeAt)
		}
		<-tick.C
	}
	// With no worker polling, the fired timer moved nothing: the run still
	// reads nap SLEEPING.
	idle, err := s.reader.GetRun(s.ctx, runID)
	must.OKf(t, err, "read run %s with no worker polling", runID)
	if got := taskRunNamed(idle, "nap").GetStatus(); got != orkstackv1.Status_STATUS_SLEEPING {
		t.Fatalf("nap is %v with no worker polling, want still SLEEPING", got)
	}

	s.startRunWorker()
	run := s.waitRun(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v (%s)", run.GetStatus(), run.GetError().GetMessage())
	}
	nap := taskRunNamed(run, "nap")
	if nap.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || nap.GetFinishedAt().AsTime().Before(wakeAt) ||
		!nap.GetSleep().GetWakeAt().AsTime().Equal(wakeAt) {
		t.Fatalf("nap = %v, want SUCCEEDED no earlier than its unchanged wake_at %v", nap, wakeAt)
	}
	if got := taskRunNamed(run, "after").GetStatus(); got != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("after = %v", got)
	}
	events := eventsOfRun(t, s, runID)
	if slept, woke := countEvents(events, "nap", orkstackv1.EventType_EVENT_TYPE_TASK_SLEEPING),
		countEvents(events, "nap", orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED); slept != 1 || woke != 1 {
		t.Fatalf("nap started sleeping %d and woke %d times across the restart, want once each", slept, woke)
	}
	writeAwaitProof(t, "sleep-I3.txt", fmt.Sprintf("run %s: nap's timer fired with no run worker; after restart: %v\n",
		runID, run.GetStatus()))
}

// The run worker is gone in the middle of a sleep and back before it wakes:
// the fresh worker replays the run, keeps the sleep's original wake-up time,
// and wakes it once, then — never earlier and never twice.
func TestIntegration_SleepSurvivesAnEngineRestartMidSleep(t *testing.T) {
	s := newAwaitStack(t, awaitStackOptions{noOrker: true})
	runID := engine.NewRunID()
	must.OKf(t, s.starter.StartRun(s.ctx, runID, sleepOnlyOrkflow("sleep-mid-restart", 8), nil,
		engine.RunPlan{Namespace: "default", Routes: map[string]string{}}), "StartRun")
	sleeping := s.waitTask(runID, "nap", orkstackv1.Status_STATUS_SLEEPING)
	wakeAt := taskRunNamed(sleeping, "nap").GetSleep().GetWakeAt().AsTime()

	s.stopRunWorker()
	if timerFired(t, s, runID) {
		t.Fatalf("nap's timer fired before the restart (wake_at %v): this case needs the worker back before the wake", wakeAt)
	}
	s.startRunWorker()
	if !time.Now().Before(wakeAt) {
		t.Logf("the worker came back at %v, after nap's wake-up %v; the restart still preceded the timer's firing", time.Now(), wakeAt)
	}
	run := s.waitRun(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v (%s)", run.GetStatus(), run.GetError().GetMessage())
	}
	nap := taskRunNamed(run, "nap")
	if nap.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || nap.GetFinishedAt().AsTime().Before(wakeAt) ||
		!nap.GetSleep().GetWakeAt().AsTime().Equal(wakeAt) {
		t.Fatalf("nap = %v, want SUCCEEDED no earlier than its unchanged wake_at %v", nap, wakeAt)
	}
	events := eventsOfRun(t, s, runID)
	if slept, woke := countEvents(events, "nap", orkstackv1.EventType_EVENT_TYPE_TASK_SLEEPING),
		countEvents(events, "nap", orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED); slept != 1 || woke != 1 {
		t.Fatalf("nap started sleeping %d and woke %d times across the restart, want once each", slept, woke)
	}
	writeAwaitProof(t, "sleep-I2.txt", fmt.Sprintf("run %s: run worker restarted mid-sleep; nap woke once at %s (wake_at %s)\n",
		runID, nap.GetFinishedAt().AsTime().Format(time.RFC3339Nano), wakeAt.Format(time.RFC3339Nano)))
}

// sleepAcrossIntegration is a sleep beside a chain long enough to cross a
// one-event continuation budget while it sleeps.
func sleepAcrossIntegration(seconds int32) *orkstackv1.Orkflow {
	return &orkstackv1.Orkflow{Name: "sleep-across", Version: "1.0.0", Tasks: []*orkstackv1.Task{
		{Id: "nap", Sleep: &orkstackv1.SleepSpec{Seconds: seconds}},
		{Id: "a", Uses: "shell"},
		{Id: "b", Uses: "shell", Needs: []string{"a"}},
		{Id: "c", Uses: "shell", Needs: []string{"b"}},
		{Id: "merge", Uses: "shell", Needs: []string{"nap", "c"}},
	}}
}

// §7.3: a sleep is not in flight, so the run carries on in a new history
// while it sleeps, and the sleep wakes at its ORIGINAL instant — once — in
// the generation that holds it then.
func TestIntegration_SleepCrossingAContinuationWakesAtItsOriginalInstant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	s := startCANStack(t, ctx, "sleep-across", false, func(_ context.Context, _ *canStack, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return &orkstackv1.ExecuteTaskResult{TaskId: in.GetTaskId(), ValuesJson: []byte(`{}`)}, nil
	})
	budget := engine.DefaultContinuationBudget(1, 1<<20, 5*time.Minute)
	runID := engine.NewRunID()
	must.OKf(t, s.starter.StartRun(ctx, runID, sleepAcrossIntegration(10), nil, s.plan(engine.DefaultHistoryMaxEvents, &budget)), "StartRun")
	executions := s.waitForSwitch(runID, 1, 90*time.Second)

	first := carryOf(t, s.history(runID, executions[1]))
	if first == nil {
		t.Fatalf("run %s's second execution carries nothing", runID)
	}
	must.OKf(t, s.client.GetWorkflow(ctx, runID, "").Get(ctx, nil), "run %s", runID)
	run, err := s.reader.GetRun(ctx, runID)
	must.OKf(t, err, "read run %s", runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v (%s)", run.GetStatus(), run.GetError().GetMessage())
	}
	nap := taskRunNamed(run, "nap")
	wakeAt := nap.GetSleep().GetWakeAt().AsTime()
	if !wakeAt.Equal(nap.GetStartedAt().AsTime().Add(10*time.Second)) || nap.GetFinishedAt().AsTime().Before(wakeAt) {
		t.Fatalf("nap started %v, wake_at %v, finished %v: want wake_at = start + 10 s and no earlier finish",
			nap.GetStartedAt().AsTime(), wakeAt, nap.GetFinishedAt().AsTime())
	}
	events, err := s.reader.Events(ctx, runID, 0)
	must.OKf(t, err, "events of %s", runID)
	slept, woke := 0, 0
	for i, e := range events {
		if want := int64(i + 1); e.GetSequence() != want {
			t.Fatalf("event %d has sequence %d: the stream has a gap or a duplicate", i, e.GetSequence())
		}
		if e.GetTaskId() != "nap" {
			continue
		}
		switch e.GetType() {
		case orkstackv1.EventType_EVENT_TYPE_TASK_SLEEPING:
			slept++
		case orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED:
			woke++
		}
	}
	if slept != 1 || woke != 1 {
		t.Fatalf("nap started sleeping %d and woke %d times across the chain, want once each", slept, woke)
	}
	// The timer that woke it was the next generation's, recreated from the
	// saved state: the first execution never recorded it firing.
	for _, event := range s.history(runID, executions[0]).GetEvents() {
		if event.GetEventType() == enums.EVENT_TYPE_TIMER_FIRED {
			t.Fatalf("generation 0 recorded nap's timer firing: the run did not carry on while it slept")
		}
	}
	writeAwaitProof(t, "sleep-I4.txt", fmt.Sprintf("run %s: %d executions; nap wake_at %s, finished %s\n",
		runID, len(executions), wakeAt.Format(time.RFC3339Nano), nap.GetFinishedAt().AsTime().Format(time.RFC3339Nano)))
}

const sleepingFixture = "testdata/history/run-sleeping-v1.json"

// Records the sleeping replay fixture from a REAL run on a versioned worker.
// Opt-in, exactly as TestIntegration_RecordAwaitedRunHistories: the fixture
// is recorded once and then ages.
func TestIntegration_RecordSleepingRunHistory(t *testing.T) {
	if os.Getenv("ORK_RECORD_GOLDEN_HISTORY") == "" {
		t.Skip("set ORK_RECORD_GOLDEN_HISTORY=1 to record " + sleepingFixture +
			" — the committed fixture is meant to age, not to be refreshed")
	}
	address := temporalAddress()
	if conn, err := net.DialTimeout("tcp", address, 2*time.Second); err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	} else {
		_ = conn.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	namespace := versioningNamespace()
	temporalClient := dialVersioningCluster(t, ctx, namespace)
	suffix := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	runQueue := "ork-sleeping-run-" + suffix
	orkerQueue := "ork-sleeping-tasks-" + suffix
	deployment := task.Deployment{Name: "ork-sleeping-" + suffix, BuildID: "v1"}

	runWorker := worker.New(temporalClient, runQueue, deployment.Apply(worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
	}))
	engine.RegisterWorkflows(runWorker)
	snapshots := newTestSnapshotStore()
	runWorker.RegisterActivityWithOptions(engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	orkerWorker := worker.New(temporalClient, orkerQueue, deployment.Apply(worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	}))
	orkerWorker.RegisterActivityWithOptions(
		func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			return &orkstackv1.ExecuteTaskResult{
				TaskId:     in.TaskId,
				ValuesJson: valuesJSON(map[string]any{"echoed_task": in.TaskId, "upstream_count": float64(len(in.Upstream))}),
			}, nil
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask})
	eventWorker := worker.New(temporalClient, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	eventWorker.RegisterActivityWithOptions(
		engine.NewEventActivities(engine.NewEventBus(), engine.NewRunStore()).WithSnapshotStore(snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	for name, w := range map[string]worker.Worker{"run": runWorker, "orker": orkerWorker, "event": eventWorker} {
		must.OKf(t, w.Start(), "start %s worker", name)
		defer w.Stop()
	}
	setCurrentVersion(t, ctx, temporalClient, deployment, runQueue, orkerQueue)
	starter := engine.NewTemporalStarterWithOrkerQueue(temporalClient, runQueue, orkerQueue)

	// a → sleep b (2 s) → c.
	wf := &orkstackv1.Orkflow{
		Name:    "sleeping-replay",
		Version: "1.0.0",
		Tasks: []*orkstackv1.Task{
			{Id: "a", Uses: "shell"},
			{Id: "b", Needs: []string{"a"}, Sleep: &orkstackv1.SleepSpec{Seconds: 2}},
			{Id: "c", Uses: "shell", Needs: []string{"b"}},
		},
	}
	runID := fmt.Sprintf("ork-sleeping-%d", time.Now().UnixNano())
	must.OKf(t, starter.StartRun(ctx, runID, wf, nil, rawPlan(orkerQueue)), "StartRun")
	must.OKf(t, temporalClient.GetWorkflow(ctx, runID, "").Get(ctx, nil), "sleeping run %s did not succeed", runID)

	recorded := &history.History{}
	iter := temporalClient.GetWorkflowHistory(ctx, runID, "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		must.OKf(t, err, "read history")
		recorded.Events = append(recorded.Events, event)
	}
	encoded, err := temporalproto.CustomJSONMarshalOptions{Indent: "  "}.Marshal(recorded)
	must.OKf(t, err, "encode history")
	must.OKf(t, os.WriteFile(sleepingFixture, append(encoded, '\n'), 0o644), "write %s", sleepingFixture)
	t.Logf("recorded %d history events for run %s into %s (deployment version %s)",
		len(recorded.Events), runID, sleepingFixture, deployment)
}
