//go:build integration

package engine_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The force cancel on a real cluster (E88 P10, design §22.7 and §22.17.9): a
// run whose engine code fails each time it is run is ended, with its record,
// its slot and its one last event; a healthy one is never terminated; an engine
// lost between the two finishes; what squats a run's force cancels is ended;
// and a cascade ends every stuck run below.

// fcStack is one deployment whose engines force-cancel: its run queue is the
// engines' own, because a force cancel acts on no other.
type fcStack struct {
	*chaosStack
	keys     *identity.Issuer
	audit    *artifacts.MemoryAuditStore
	deps     engine.ForceCancelDeps
	settings engine.ForceCancelSettings
	force    *engine.ForceCancelStarter

	mu sync.Mutex
	// terminal are the last events published of each run, by sequence.
	terminal map[string][]int64
}

func newFCStack(t *testing.T, ctx context.Context, scenario string) *fcStack {
	t.Helper()
	cfg := s3Config()
	requireObjectStore(t, cfg)
	local, err := artifacts.NewLocalRunArchive(t.TempDir())
	must.OK(t, err)
	s := newChaosStackWith(t, ctx, scenario, cfg, local)
	for _, ensure := range []func(context.Context, client.Client, string) (bool, error){task.EnsureRunSearchAttributes, task.EnsureRunOriginSearchAttributes,
		task.EnsureRunRootSearchAttribute, task.EnsureConcurrencySearchAttributes} {
		usable, err := ensure(ctx, s.control, s.namespace)
		if err != nil || !usable {
			t.Fatalf("the cluster's index is not usable in %s: %v", s.namespace, err)
		}
	}
	published, err := artifacts.NewPublishedKeysStore(ctx, artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	must.OK(t, err)
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	must.OK(t, err)
	signer, err := jwt.NewSigner(k)
	must.OK(t, err)
	keys, err := identity.New(identity.Config{Keys: []*jwt.Signer{signer}, Algorithm: jwt.ES256, ClockSkew: time.Minute, Engine: "engine-a"}, published, nil, nil)
	must.OK(t, err)
	must.OK(t, keys.Publish(ctx))
	enforce := func() string { return "enforce" }

	f := &fcStack{chaosStack: s, keys: keys, audit: artifacts.NewMemoryAuditStore(), terminal: map[string][]int64{}}
	s.runQueue = task.DefaultQueue
	s.starter = engine.NewTemporalStarterWithOrkerQueue(s.control, s.runQueue, s.orkerQueue).WithNamespaceIndex(true).WithOriginIndex(true).
		WithRootIndex(true).WithConcurrencyIndex(true).WithClusterIdentity(s.namespace, "engine-a")
	s.reader = engine.NewTemporalRunReader(s.control).WithSnapshotStore(s.store).WithNamespaceIndex(true).WithOriginIndex(true).WithRootIndex(true).
		WithForceCancelVerifier(keys, enforce)
	s.archive = engine.NewRunArchiver(local).WithForceCancelVerifier(keys, enforce)
	f.settings = engine.DefaultForceCancelSettings()
	// The least they may be, so a test waits little more than one grace.
	f.settings.Grace = 30 * time.Second
	f.settings.Cascade.Poll = time.Second
	f.deps = engine.ForceCancelDeps{Client: s.control, ClusterNamespace: s.namespace, Starter: s.starter, Reader: s.reader, Snapshots: s.store,
		Keys: keys, Mode: enforce, Audit: f.audit, Settings: f.settings}
	f.force = engine.NewForceCancelStarter(f.deps, f.settings)
	return f
}

// steps are an engine's local activities beside a run's own.
func (f *fcStack) steps() replicaOptions {
	return replicaOptions{extra: []func(worker.Worker){func(w worker.Worker) {
		engine.RegisterForceCancelActivities(w, engine.NewForceCancelActivities(f.deps))
		engine.RegisterGateActivities(w, engine.NewGateActivities(f.control))
	}}}
}

// poisoned registers the engine's workflows with a RunWorkflow that fails its
// workflow task each time, as a release that cannot replay a run does.
type poisoned struct{ worker.Worker }

func (p poisoned) RegisterWorkflowWithOptions(w any, o workflow.RegisterOptions) {
	if o.Name == engine.RunWorkflowType {
		// Its arguments are its own: one they could not be read into would
		// close it instead.
		w = reflect.MakeFunc(reflect.TypeOf(w), func([]reflect.Value) []reflect.Value {
			panic("this release cannot run this run")
		}).Interface()
	}
	p.Worker.RegisterWorkflowWithOptions(w, o)
}

// startPoisoned starts an engine replica whose runs all fail.
func (f *fcStack) startPoisoned(name string) *chaosReplica {
	f.t.Helper()
	c := f.dial(name, "")
	w := worker.New(c, f.runQueue, worker.Options{DisableRegistrationAliasing: true, LocalActivityWorkerOnly: true, Identity: name, Interceptors: engine.RunWorkerInterceptors()})
	engine.RegisterWorkflows(poisoned{w})
	registerRunActivities(w, engine.NewSnapshotActivities(f.store), f.values)
	for _, register := range f.steps().extra {
		register(w)
	}
	must.OKf(f.t, w.Start(), "start poisoned replica %s", name)
	r := &chaosReplica{name: name, client: c, worker: w, store: f.store, reader: f.reader}
	f.t.Cleanup(r.stop)
	f.transcript.step("poisoned replica %s started", name)
	return r
}

// startEvents starts the engines' event worker: the projection, counting the
// last events it publishes, and the force cancel's build.
func (f *fcStack) startEvents() {
	f.t.Helper()
	w := worker.New(f.control, engine.EventQueue, worker.Options{DisableRegistrationAliasing: true, DisableWorkflowWorker: true})
	publish := engine.NewArchivingEventActivities(f.bus, f.runs, f.archive).WithEventSource(f.reader).WithSnapshotStore(f.store)
	w.RegisterActivityWithOptions(func(ctx context.Context, e *orkstackv1.Event, uri, key string) error {
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED || e.GetType() == orkstackv1.EventType_EVENT_TYPE_RUN_FAILED {
			f.mu.Lock()
			f.terminal[e.GetRunId()] = append(f.terminal[e.GetRunId()], e.GetSequence())
			f.mu.Unlock()
		}
		return publish.PublishEvent(ctx, e, uri, key)
	}, activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	engine.RegisterForceCancelPublish(w, engine.NewForceCancelActivities(f.deps))
	must.OKf(f.t, w.Start(), "start the event worker")
	f.t.Cleanup(w.Stop)
}

// ask is a request for a force cancel of runID by a verified caller.
func (f *fcStack) ask(runID string) engine.ForceCancelAsk {
	return engine.ForceCancelAsk{Namespace: "default", RunID: runID, Reason: "stuck after the roll", Principal: "user:corp:ada", PrincipalVerified: true,
		AuthMethod: "oidc", PrincipalIssuer: "https://idp", Permission: "run.cancel.force", Scope: "orkflow:default/stuck", Rule: "ops", PolicyGeneration: 7}
}

// over waits until the run's force cancel is over, and answers what it reports.
func (f *fcStack) over(runID, requestID string, within time.Duration) engine.ForceCancelStatus {
	f.t.Helper()
	deadline := time.Now().Add(within)
	for {
		got, err := f.force.Status(f.ctx, "default", runID, requestID)
		if err == nil && !got.Running {
			f.transcript.step("force cancel %s of %s: %s %s", requestID, runID, got.State, got.Cause)
			return got
		}
		if time.Now().After(deadline) {
			f.t.Fatalf("the force cancel of %s is not over after %s: %+v, %v", runID, within, got, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// indexed waits until the cluster's index, which lags a start, holds n force
// cancels of the run, by the prefix query they are found by.
func (f *fcStack) indexed(runID string, n int64) {
	f.t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		got, err := f.control.CountWorkflow(f.ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: f.namespace,
			Query: fmt.Sprintf("WorkflowType = %q AND WorkflowId STARTS_WITH %q", engine.ForceCancelWorkflowType, engine.ForceCancelWorkflowIDPrefix("default", runID))})
		must.OK(f.t, err)
		if got.GetCount() == n {
			return
		}
		if time.Now().After(deadline) {
			f.t.Fatalf("the index holds %d of %d", got.GetCount(), n)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// failing waits until the run's workflow task has failed, so the run is the
// loop the test is about and not one that has simply not been looked at.
func (f *fcStack) failing(runID string) {
	f.t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		for _, e := range f.history(runID, "").GetEvents() {
			// A reset records one too, which no engine failed.
			if a := e.GetWorkflowTaskFailedEventAttributes(); a != nil && a.GetCause() != enums.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW {
				return
			}
		}
		if time.Now().After(deadline) {
			f.t.Fatalf("run %s's workflow task never failed", runID)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// poke gives the run a workflow task, which a poisoned engine fails.
func (f *fcStack) poke(runID string) {
	f.t.Helper()
	must.OK(f.t, f.control.SignalWorkflow(f.ctx, runID, "", "poke", nil))
}

// records are the actions the audit trail holds of target.
func (f *fcStack) records(target string) map[string]*orkstackv1.AuditRecord {
	f.t.Helper()
	page, err := f.audit.ListAuditRecords(f.ctx, artifacts.AuditQuery{Since: time.Now().Add(-time.Hour), Until: time.Now().Add(time.Hour), Limit: 200})
	must.OK(f.t, err)
	out := map[string]*orkstackv1.AuditRecord{}
	for _, rec := range page.Records {
		if rec.GetTarget() == target {
			out[rec.GetAction()] = rec
		}
	}
	return out
}

// lastEvents are the distinct sequences of the run's published last events.
func (f *fcStack) lastEvents(runID string) []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := slices.Clone(f.terminal[runID])
	slices.Sort(out)
	return slices.Compact(out)
}

// stuckDoc is a run whose one task is in a pin and held; its cancellation
// grace is the least, so a force cancel's is too.
const stuckDoc = `apiVersion: orkstack/v1
kind: Orkflow
name: stuck
version: "1.0.0"
pins:
  here:
    timeout_seconds: 600
tasks:
  - id: hold
    uses: shell
    pin: here
    cancellation_grace_period_seconds: 1
    with: {op: exec}
`

// TestIntegration_ForceCancelStalledRun: a run in a workflow-task failure loop,
// holding a concurrency slot and an open pin, ends CANCELED RUN_FORCE_CANCELED
// with who asked and why, one last event, its slot free at once and its pin
// left to its lease; it reads the same live and archived; and asking again
// adds nothing.
func TestIntegration_ForceCancelStalledRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	f := newFCStack(t, ctx, "fc-stalled")
	held := newGates()
	f.startOrker("orker-1", gated(held, "hold"))
	f.startEvents()
	healthy := f.startReplica("engine-1", f.steps())

	key := fmt.Sprintf("fc-%d", time.Now().UnixNano())
	gate := engine.GateID("default", "", key)
	plan := f.plan(engine.DefaultHistoryMaxEvents, engine.DefaultHistoryMaxBytes, nil)
	run := 0
	tunables := gateTunablesForTest()
	plan.Concurrency = &engine.ConcurrencyPlan{Run: &run, SeqBase: 1, GateQueue: f.runQueue, Namespace: "default", Gate: tunables,
		Slots:          []engine.ConcurrencySlotPlan{{GateID: gate, Key: key, Label: key, Limit: 1, Strategy: engine.ConcurrencyStrategyQueue, QueueTimeoutSeconds: 600}},
		AcquireTimeout: 20 * time.Second, AcquireBackoffMin: 200 * time.Millisecond, AcquireBackoffMax: 2 * time.Second,
		RequeueInterval: time.Minute, RequeueBackoffFactor: 2, RequeueIntervalMax: 4 * time.Minute, PerSlotBytes: 2048, EventHoldersShown: 5}
	runID := fmt.Sprintf("ork-fc%d", time.Now().UnixNano())
	f.transcript.run(runID)
	f.start(runID, compile(t, stuckDoc), nil, plan)
	stream := f.openStream(f.reader, runID, 0)
	held.waitEntered(t, "hold", 2*time.Minute)
	view := func() (engine.GateStateView, error) {
		var v engine.GateStateView
		value, err := f.control.QueryWorkflow(ctx, gate, "", engine.GateQueryState)
		if err == nil {
			err = value.Get(&v)
		}
		return v, err
	}
	if v, err := view(); err != nil || !slices.Equal(holders(v), []string{runID}) {
		t.Fatalf("the gate is held by %v, %v; want %s", holders(v), err, runID)
	}
	healthy.stop()
	f.startPoisoned("engine-2")
	f.poke(runID)
	f.failing(runID)
	before := f.history(runID, "")

	began := time.Now()
	answer, err := f.force.Request(ctx, f.ask(runID))
	must.OK(t, err)
	if answer.State != engine.FCStateRequested || answer.Attached || answer.JoinSkipped {
		t.Fatalf("answered %+v", answer)
	}
	// Another request joins it.
	f.indexed(runID, 1)
	joined, err := f.force.Request(ctx, f.ask(runID))
	must.OK(t, err)
	if !joined.Attached || joined.RequestID != answer.RequestID {
		t.Fatalf("the second request answered %+v", joined)
	}
	got := f.over(runID, answer.RequestID, 4*time.Minute)
	if got.State != engine.FCStateForceCanceled || got.Cause != "" || got.Incomplete {
		t.Fatalf("%+v", got)
	}
	if took := time.Since(began); took < f.settings.Grace {
		t.Errorf("ended after %s, less than its grace", took)
	}
	if _, status := f.current(runID); status != enums.WORKFLOW_EXECUTION_STATUS_TERMINATED {
		t.Fatalf("the run closed %s", status)
	}
	// Its slot is free at once: the gate's own sweep is an hour away.
	// A gate nothing holds or waits for may have closed.
	v, err := view()
	if err != nil {
		d, derr := describeExecution(f.control, ctx, gate, "")
		if derr != nil || d.GetWorkflowExecutionInfo().GetStatus() != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED {
			t.Errorf("the gate is unread (%v) and not closed: %v, %v", err, d.GetWorkflowExecutionInfo().GetStatus(), derr)
		}
	} else if len(holders(v)) != 0 {
		t.Errorf("the gate is held by %v", holders(v))
	}
	// No code of the run ran: it finished no workflow task since it was stuck,
	// so nothing was scheduled, its pin's release included.
	for _, e := range f.history(runID, "").GetEvents()[len(before.GetEvents()):] {
		if e.GetEventType() == enums.EVENT_TYPE_WORKFLOW_TASK_COMPLETED || e.GetActivityTaskScheduledEventAttributes() != nil {
			t.Errorf("%s at %d, after it was stuck", e.GetEventType(), e.GetEventId())
		}
	}
	events := f.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_CANCELED, engine.RunForceCanceledCode)
	last := events[len(events)-1]
	read, err := f.reader.GetRun(ctx, runID)
	must.OK(t, err)
	if read.GetCanceledBy() != "user:corp:ada" || !read.GetCanceledByVerified() || !strings.Contains(read.GetError().GetMessage(), "stuck after the roll") || last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED {
		t.Errorf("the run reads %s, %v, %v; its last event is %v", read.GetCanceledBy(), read.GetCanceledByVerified(), read.GetError(), last.GetType())
	}
	if published := f.lastEvents(runID); !slices.Equal(published, []int64{last.GetSequence()}) {
		t.Errorf("published last events %v, want one, %d", published, last.GetSequence())
	}
	records := f.records(runID)
	for _, action := range []string{"run.cancel.force.requested", "run.cancel.force.completed"} {
		if rec := records[action]; rec.GetPrincipal() != "user:corp:ada" || !rec.GetPrincipalVerified() || !rec.GetSucceeded() {
			t.Errorf("%s: %v", action, rec)
		}
	}
	// Asked again, it changes nothing.
	again, err := f.force.Request(ctx, f.ask(runID))
	must.OK(t, err)
	if got := f.over(runID, again.RequestID, 2*time.Minute); got.State != engine.FCStateAlreadyEnded || got.Cause != "" || got.Incomplete {
		t.Errorf("asked again: %+v", got)
	}
	if published := f.lastEvents(runID); !slices.Equal(published, []int64{last.GetSequence()}) {
		t.Errorf("asking again published %v", published)
	}
}

// TestIntegration_ForceCancelHealthyLongGraceNotTerminated: a healthy run whose
// task takes most of its own cancellation grace to stop ends by its own cancel,
// and is never terminated.
func TestIntegration_ForceCancelHealthyLongGraceNotTerminated(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	f := newFCStack(t, ctx, "fc-healthy")
	entered := make(chan struct{})
	var once sync.Once
	f.startOrker("orker-1", func(ctx context.Context, _ *chaosStack, _ string, _ *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		once.Do(func() { close(entered) })
		beat := time.NewTicker(time.Second)
		defer beat.Stop()
		for {
			select {
			case <-ctx.Done():
				// Its container traps the signal: longer than the deployment's
				// own least grace, and within the task's, beating throughout as
				// an Orker does.
				stopped, alive := time.After(f.settings.Grace+10*time.Second), context.WithoutCancel(ctx)
				for {
					select {
					case <-stopped:
						return nil, ctx.Err()
					case <-beat.C:
						activity.RecordHeartbeat(alive)
					}
				}
			case <-beat.C:
				activity.RecordHeartbeat(ctx)
			}
		}
	})
	f.startEvents()
	f.startReplica("engine-1", f.steps())
	runID := f.recordedRunID()
	f.start(runID, compile(t, `apiVersion: orkstack/v1
kind: Orkflow
name: healthy
version: "1.0.0"
tasks:
  - id: slow
    uses: shell
    cancellation_grace_period_seconds: 60
    with: {op: exec}
`), nil, f.plan(engine.DefaultHistoryMaxEvents, engine.DefaultHistoryMaxBytes, nil))
	stream := f.openStream(f.reader, runID, 0)
	select {
	case <-entered:
	case <-time.After(2 * time.Minute):
		t.Fatal("the task never started")
	}
	answer, err := f.force.Request(ctx, f.ask(runID))
	must.OK(t, err)
	if answer.Decision.Grace() != 90*time.Second {
		t.Errorf("its grace is %s, want its task's and the margin", answer.Decision.Grace())
	}
	if got := f.over(runID, answer.RequestID, 5*time.Minute); got.State != engine.FCStateCanceled {
		t.Fatalf("%+v", got)
	}
	if _, status := f.current(runID); status != enums.WORKFLOW_EXECUTION_STATUS_CANCELED {
		t.Fatalf("the run closed %s", status)
	}
	f.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_CANCELED, "RUN_CANCELED")
}

// TestIntegration_ForceCancelEngineKilledAfterTerminate: the engine lost between
// the termination and the last event, and no event worker until then: the
// engine that takes over publishes the event, once.
func TestIntegration_ForceCancelEngineKilledAfterTerminate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	f := newFCStack(t, ctx, "fc-killed")
	held := newGates()
	f.startOrker("orker-1", gated(held, "hold"))
	healthy := f.startReplica("engine-1", f.steps())
	projection := f.startProjection(nil)
	runID := f.recordedRunID()
	f.start(runID, compile(t, stuckDoc), nil, f.plan(engine.DefaultHistoryMaxEvents, engine.DefaultHistoryMaxBytes, nil))
	stream := f.openStream(f.reader, runID, 0)
	held.waitEntered(t, "hold", 2*time.Minute)
	healthy.stop()
	projection.Stop()
	first := f.startPoisoned("engine-2")
	f.poke(runID)
	f.failing(runID)
	answer, err := f.force.Request(ctx, f.ask(runID))
	must.OK(t, err)
	deadline := time.Now().Add(3 * time.Minute)
	for {
		if _, status := f.current(runID); status == enums.WORKFLOW_EXECUTION_STATUS_TERMINATED {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the run was not terminated")
		}
		time.Sleep(50 * time.Millisecond)
	}
	// The event waits for a worker nobody runs, so it is not out when the
	// engine is lost.
	fc := engine.ForceCancelWorkflowIDPrefix("default", runID) + answer.RequestID
	for {
		d, err := describeExecution(f.control, ctx, fc, "")
		if err == nil && len(d.GetPendingActivities()) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the force cancel never asked for its event: %v, %v", d.GetPendingActivities(), err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	first.stop()
	f.transcript.step("engine-2 killed after the termination")
	f.startPoisoned("engine-3")
	f.startEvents()
	if got := f.over(runID, answer.RequestID, 4*time.Minute); got.State != engine.FCStateForceCanceled || got.Incomplete {
		t.Fatalf("%+v", got)
	}
	events := f.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_CANCELED, engine.RunForceCanceledCode)
	if published := f.lastEvents(runID); !slices.Equal(published, []int64{events[len(events)-1].GetSequence()}) {
		t.Errorf("published last events %v", published)
	}
}

// TestIntegration_ForceCancelSquattedId: what is started first under a run's
// force cancels' ids, on a channel nobody takes work from, is ended and
// recorded, joined by nobody, and stops nothing. It is also the proof that the
// cluster's index answers the prefix query a run's force cancels are found by.
func TestIntegration_ForceCancelSquattedId(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	f := newFCStack(t, ctx, "fc-squat")
	held := newGates()
	f.startOrker("orker-1", gated(held, "hold"))
	f.startEvents()
	healthy := f.startReplica("engine-1", f.steps())
	runID := f.recordedRunID()
	f.start(runID, compile(t, stuckDoc), nil, f.plan(engine.DefaultHistoryMaxEvents, engine.DefaultHistoryMaxBytes, nil))
	held.waitEntered(t, "hold", 2*time.Minute)
	healthy.stop()
	f.startPoisoned("engine-2")
	prefix := engine.ForceCancelWorkflowIDPrefix("default", runID)
	var squatters []client.WorkflowRun
	for i := range 3 {
		w, err := f.control.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: fmt.Sprintf("%ssquat-%d", prefix, i), TaskQueue: "ork-dead"},
			engine.ForceCancelWorkflowType, engine.ForceCancelInput{Unsigned: &engine.ForceCancelDecision{RequestID: fmt.Sprintf("squat-%d", i), Namespace: "default", RunID: runID, Principal: "mallory"}})
		must.OK(t, err)
		squatters = append(squatters, w)
	}
	f.indexed(runID, 3)
	answer, err := f.force.Request(ctx, f.ask(runID))
	must.OK(t, err)
	if answer.State != engine.FCStateRequested || answer.Squatters != 3 {
		t.Fatalf("%+v", answer)
	}
	for _, w := range squatters {
		d, err := describeExecution(f.control, ctx, w.GetID(), w.GetRunID())
		if err != nil || d.GetWorkflowExecutionInfo().GetStatus() != enums.WORKFLOW_EXECUTION_STATUS_TERMINATED {
			t.Errorf("%s is %v, %v", w.GetID(), d.GetWorkflowExecutionInfo().GetStatus(), err)
		}
	}
	if f.records(runID)["run.cancel.force.squatter"] == nil {
		t.Error("no squatter was recorded")
	}
	if got := f.over(runID, answer.RequestID, 4*time.Minute); got.State != engine.FCStateForceCanceled {
		t.Fatalf("%+v", got)
	}
	// The newest of a run is found by the same query.
	if got, err := f.force.Status(ctx, "default", runID, ""); err != nil || got.Decision.RequestID != answer.RequestID {
		t.Errorf("%+v, %v", got, err)
	}
}

// TestIntegration_ForceCancelCascade: a stuck run whose child run is stuck too:
// the cascade ends both, each with its own record, and says so.
func TestIntegration_ForceCancelCascade(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	f := newFCStack(t, ctx, "fc-cascade")
	runID, childID := f.stuckTree()
	ask := f.ask(runID)
	ask.Cascade = true
	answer, err := f.force.Request(ctx, ask)
	must.OK(t, err)
	got := f.over(runID, answer.RequestID, 6*time.Minute)
	if got.State != engine.FCStateForceCanceled || got.Incomplete || got.Cascade == nil || got.Cascade.State != engine.FCCascadeComplete {
		t.Fatalf("%+v, cascade %+v", got, got.Cascade)
	}
	if rows := got.Cascade.Results; len(rows) != 1 || rows[0].RunID != childID || rows[0].State != engine.FCStateForceCanceled || !strings.HasPrefix(rows[0].RequestID, answer.RequestID+"-") {
		t.Errorf("rows %+v", rows)
	}
	for _, id := range []string{runID, childID} {
		run, err := f.reader.GetRun(ctx, id)
		if err != nil || run.GetError().GetCode() != engine.RunForceCanceledCode || run.GetCanceledBy() != "user:corp:ada" || !run.GetCanceledByVerified() {
			t.Errorf("%s reads %v by %s, %v", id, run.GetError(), run.GetCanceledBy(), err)
		}
	}
	if rec := f.records(runID)["run.cancel.force.cascade"]; !rec.GetSucceeded() || !strings.Contains(rec.GetDetail(), "complete, force_canceled=1, open after it=0") {
		t.Errorf("%v", rec)
	}
}
