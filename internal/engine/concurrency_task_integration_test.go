//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The task side of E88 P7's concurrency limit, on a real cluster and a real
// gate. Each asserts outcomes: what runs, when, and what the gate holds.

// taskLimitPlan is the stack's plan with one task's own limit of one on key,
// as submit pins it, and none of the run's own.
func taskLimitPlan(s *canStack, key, taskID string, queueTimeout int64) engine.RunPlan {
	plan := concurrencyRunPlan(s, key, queueTimeout, time.Minute, nil, nil)
	plan.Concurrency.Run, plan.Concurrency.Tasks = nil, map[string]int{taskID: 0}
	return plan
}

// taskLimitDocument is a run whose hold task declares a limit of one on key,
// beside a task, free, that needs nothing and has no limit.
func taskLimitDocument(name, key string) string {
	return fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
version: "1.0.0"
tasks:
  - id: hold
    uses: shell
    with: {op: exec}
    concurrency: {limit: 1, key: %q}
  - id: free
    uses: shell
    with: {op: exec}
`, name, key)
}

// taskHolderFault is what a gate's state may never show under a limit of
// one: two holders, or a run's entry twice.
func taskHolderFault(v engine.GateStateView) string {
	if len(v.State.Holders) > 1 {
		return fmt.Sprintf("holders %v exceed the limit of one", holders(v))
	}
	return gateFault(v)
}

// TestIntegration_ConcurrencyTaskWaitsAndItsRunGoesOn: a task whose key
// another run's task holds waits in line, PENDING with its place, while its
// run stays RUNNING and its other task runs; it starts only once the holder
// ends, the two never hold at once, and the gate is left empty.
func TestIntegration_ConcurrencyTaskWaitsAndItsRunGoesOn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	s := newConcRun(t, ctx, "conc-task", nil)
	key := s.key("task")
	gate := s.gate(key)
	a, b := s.id("ta"), s.id("tb")
	plan := taskLimitPlan(s.canStack, key, "hold", 180)

	s.start(a, taskLimitDocument("conc-task", key), plan)
	s.waitFor(a+" holding", func() bool { return s.holds.holding(a) })
	s.start(b, taskLimitDocument("conc-task", key), plan)
	v := s.waitGate(gate, b+"'s task in line", func(v engine.GateStateView) bool { return slices.Equal(waiting(v), []string{b}) })
	if fault := taskHolderFault(v); fault != "" || len(v.State.Queue) != 1 || v.State.Queue[0].TaskIx != 0 ||
		!slices.Equal(holders(v), []string{a}) {
		t.Fatalf("gate %+v (%s); want %s's task holding and %s's in line", v.State, fault, a, b)
	}
	s.waitFor(b+"'s free task", func() bool {
		return s.countEvents(b, orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED, "") >= 1
	})
	s.waitFor(b+"'s TASK_QUEUED", func() bool { return s.countEvents(b, orkstackv1.EventType_EVENT_TYPE_TASK_QUEUED, "") == 1 })
	run, err := s.reader.GetRun(ctx, b)
	must.OKf(t, err, "GetRun %s", b)
	hold := taskRunOfRun(run, "hold")
	if run.GetStatus() != orkstackv1.Status_STATUS_RUNNING || run.GetQueued() != nil ||
		hold.GetStatus() != orkstackv1.Status_STATUS_PENDING || hold.GetQueued().GetKey() != key || taskRunOfRun(run, "free").GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run %s is %s (queued %v), hold %v; want RUNNING, its task waiting and free done", b, run.GetStatus(), run.GetQueued(), hold)
	}
	if s.holds.holding(b) {
		t.Fatalf("%s's task started while %s's held the key", b, a)
	}

	s.holds.let(a)
	s.mustEnd(a)
	s.waitFor(b+" holding", func() bool { return s.holds.holding(b) })
	if v, ok := s.gateView(gate); ok {
		if fault := taskHolderFault(v); fault != "" || !slices.Equal(holders(v), []string{b}) {
			t.Fatalf("after %s ended, gate %+v (%s); want %s's task alone", a, v.State, fault, b)
		}
	}
	s.holds.let(b)
	s.mustEnd(b)
	s.waitGate(gate, "the gate empty", isEmpty)
	if n := s.countEvents(a, orkstackv1.EventType_EVENT_TYPE_TASK_QUEUED, ""); n > 1 {
		t.Fatalf("%s published %d TASK_QUEUED events, want at most its first arrival's", a, n)
	}
}

// TestIntegration_ConcurrencyResumedRunRetakesItsTasksSlot: a run whose only
// limit is its task's is resumed; the task run again waits in line RUNNING
// behind another run's task on its key, under the resume's incarnation, and
// runs once that one ends. The two never run at once, and the resume leaves
// nothing of any incarnation at the gate.
func TestIntegration_ConcurrencyResumedRunRetakesItsTasksSlot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	stamp := strconv.FormatInt(time.Now().UnixNano(), 36)
	runID, otherID := "ork-tresumed-"+stamp, "ork-tother-"+stamp
	commits := artifacts.NewMemoryCommitStore()
	release := make(chan struct{})
	var mu sync.Mutex
	deploys, running := 0, map[string]bool{}
	var overlap string
	behavior := func(actx context.Context, _ *canStack, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.GetTaskId() == "deploy" {
			mu.Lock()
			for other := range running {
				if other != in.GetRunId() {
					overlap = fmt.Sprintf("%s's deploy ran while %s's did", in.GetRunId(), other)
				}
			}
			running[in.GetRunId()] = true
			mu.Unlock()
			defer func() { mu.Lock(); delete(running, in.GetRunId()); mu.Unlock() }()
		}
		if in.GetRunId() == otherID && in.GetTaskId() == "deploy" {
			select {
			case <-release:
			case <-actx.Done():
				return nil, actx.Err()
			}
		}
		if in.GetRunId() == runID && in.GetTaskId() == "deploy" {
			mu.Lock()
			deploys++
			n := deploys
			mu.Unlock()
			if n == 1 {
				return nil, temporal.NewNonRetryableApplicationError("deploy failed", "ActivityFailed", nil)
			}
		}
		record, err := commits.Create(actx, in.GetCommitUri(), &orkstackv1.ActivityCommit{
			SchemaVersion: artifacts.CommitSchemaVersion, Namespace: artifacts.ResolveNamespace(in.GetNamespace()),
			RunId: in.GetRunId(), TaskId: in.GetTaskId(), OrkflowDefinitionDigest: in.GetOrkflowDefinitionDigest(),
			IdempotencyKey: in.GetIdempotencyKey(), Image: in.GetImage(), Attempt: 1, OrkerId: "orker-it",
		})
		if err != nil {
			return nil, err
		}
		result := succeeded(in.GetTaskId(), map[string]any{"ok": true})
		result.Commit = &orkstackv1.CommitRef{Uri: in.GetCommitUri(), Sha256: record.SHA256}
		return result, nil
	}
	records, err := artifacts.NewRecordStore(ctx, s3Config())
	must.OKf(t, err, "record store")
	gateClient := dialVersioningCluster(t, ctx, versioningNamespace())
	var s *canStack
	s = startCANStack(t, ctx, "resume-task", true, behavior, func(w worker.Worker) {
		engine.RegisterGateActivities(w, engine.NewGateActivities(gateClient))
		acts := engine.NewResumeActivities(commits, nil).WithHistory(lazyHistory{&s}, lazyNamespace(&s), records)
		w.RegisterActivityWithOptions(acts.ReadResetFacts, activity.RegisterOptions{Name: engine.ActivityReadResetFacts})
		w.RegisterActivityWithOptions(acts.LoadResumePlan, activity.RegisterOptions{Name: engine.ActivityLoadResumePlan})
		w.RegisterActivityWithOptions(acts.AdoptRecorded, activity.RegisterOptions{Name: engine.ActivityAdoptRecorded})
	})
	for what, ensure := range map[string]func() (bool, error){
		"namespace":   func() (bool, error) { return task.EnsureRunSearchAttributes(ctx, s.client, s.namespace) },
		"concurrency": func() (bool, error) { return task.EnsureConcurrencySearchAttributes(ctx, s.client, s.namespace) },
	} {
		usable, err := ensure()
		must.OKf(t, err, "register the %s attributes", what)
		if !usable {
			t.Fatalf("the %s attributes are not usable", what)
		}
	}
	c := &concRun{canStack: s, ctx: ctx, stamp: stamp}
	key := c.key("tresume")
	gate := c.gate(key)
	plan := taskLimitPlan(s, key, "deploy", 180)
	c.start(runID, resumeTwoTaskDocument, plan)
	if err := c.end(runID); err == nil {
		t.Fatal("the base run succeeded; deploy was meant to fail")
	}

	store, err := artifacts.NewLocalStore(t.TempDir())
	must.OKf(t, err, "artifact store")
	endings, err := artifacts.NewRunArchive(s3Config())
	must.OKf(t, err, "run archive")
	archiver := engine.NewRunArchiver(endings)
	s.reader.WithEndings(archiver)
	seam := s.starter.WithConcurrencyIndex(true).WithClusterIdentity(s.namespace, "ork-resume-it")
	settings := api.DefaultResumeSettings()
	settings.ConfirmTimeout = 2 * time.Minute
	svc := api.NewService(api.Config{
		ResumeSettings: settings, Reader: s.reader, Starter: seam, Pools: servedPools{}, Commits: commits, ArtifactStore: store,
		Attempts: artifacts.NewMemoryAttemptStore(), Resume: seam, ResumeRecords: records,
		ResumeClaims: &resumeClaims{claims: map[string]engine.Claim{}}, ResumeSnapshots: s.snapshots,
		ResumeEndings: endings.(artifacts.RunEndingStore), ResumeArchiver: engine.NewHistoryArchiver(s.reader, archiver),
		Releases:          runningRelease{build: s.deployment.BuildID},
		ConcurrencyReader: engine.NewTemporalConcurrencyReader(s.client, 0), Capabilities: everyEngineHolds{},
		Now: func() time.Time { return time.Now().Add(time.Hour) },
	})

	// Another run's deploy holds the key; the resume's must wait for it.
	c.start(otherID, resumeTwoTaskDocument, plan)
	c.waitGate(gate, "the other run's task holding", func(v engine.GateStateView) bool { return slices.Equal(holders(v), []string{otherID}) })
	preview, err := svc.ResumeRun(ctx, connect.NewRequest(&orkstackv1.ResumeRunRequest{RunId: runID, DryRun: true}))
	must.OKf(t, err, "preview the resume of a run whose task alone has a limit")
	_, err = svc.ResumeRun(ctx, connect.NewRequest(&orkstackv1.ResumeRunRequest{RunId: runID,
		ExpectedTerminalSequence: preview.Msg.GetTerminalSequence(), ExpectedPlanDigest: preview.Msg.GetPlanDigest()}))
	must.OKf(t, err, "confirm the resume")
	d, err := describeExecution(s.client, ctx, runID, "")
	must.OKf(t, err, "describe the resumed run")
	resumed := d.GetWorkflowExecutionInfo().GetExecution().GetRunId()

	v := c.waitGate(gate, "the resumed run's task in line", func(v engine.GateStateView) bool { return slices.Contains(waiting(v), runID) })
	if fault := taskHolderFault(v); fault != "" {
		t.Fatal(fault)
	}
	for _, e := range v.State.Queue {
		if e.Run == runID && e.SeqBase != int64(preview.Msg.GetSeqBase()) {
			t.Fatalf("the resumed task waits under incarnation %d, want the resume's %d", e.SeqBase, preview.Msg.GetSeqBase())
		}
	}
	run, err := s.reader.GetRun(ctx, runID)
	must.OKf(t, err, "read the resumed run")
	if deploy := taskRunOfRun(run, "deploy"); run.GetStatus() != orkstackv1.Status_STATUS_RUNNING ||
		deploy.GetStatus() != orkstackv1.Status_STATUS_PENDING || deploy.GetQueued().GetKey() != key {
		t.Fatalf("resumed run %s, deploy %v; want RUNNING with deploy waiting for key %s", run.GetStatus(), deploy, key)
	}
	close(release)
	c.mustEnd(otherID)
	must.OKf(t, s.client.GetWorkflow(ctx, runID, resumed).Get(ctx, nil), "the resumed run")
	mu.Lock()
	defer mu.Unlock()
	if deploys != 2 || overlap != "" {
		t.Fatalf("deploy ran %d times (want the base's and the resume's); %s", deploys, overlap)
	}
	c.waitGate(gate, "nothing held or waiting", isEmpty)
}
