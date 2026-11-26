//go:build integration

package engine_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// everyEngineHolds is a fleet whose engines all hold concurrency limits.
type everyEngineHolds struct{ engine.CapabilityChecker }

func (everyEngineHolds) MissingCapability(context.Context, string, engine.CapabilityScope) ([]string, error) {
	return nil, nil
}

// TestIntegration_ConcurrencyResumedRunAcquiresAfterResultsKept: a resumed run
// asks for its slot anew, after RUN_RESULTS_KEPT and in a workflow task after
// the one that released its earlier incarnations; a key another run holds
// makes it wait in line RUNNING, and it goes on once granted. A second resume
// does the same, and leaves nothing of any incarnation behind.
func TestIntegration_ConcurrencyResumedRunAcquiresAfterResultsKept(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	stamp := strconv.FormatInt(time.Now().UnixNano(), 36)
	runID, otherID := "ork-resumed-"+stamp, "ork-other-"+stamp
	commits := artifacts.NewMemoryCommitStore()
	release := make(chan struct{})
	var mu sync.Mutex
	deploys := 0
	behavior := func(actx context.Context, _ *canStack, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.GetRunId() == otherID {
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
			if n < 3 {
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
	s = startCANStack(t, ctx, "resume-conc", true, behavior, func(w worker.Worker) {
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
	key := c.key("resume")
	gate := c.gate(key)
	plan := concurrencyRunPlan(s, key, 180, time.Minute, nil, nil)
	c.start(runID, resumeTwoTaskDocument, plan)
	if err := c.end(runID); err == nil {
		t.Fatal("the base run succeeded; deploy was meant to fail")
	}

	store, err := artifacts.NewLocalStore(t.TempDir())
	must.OKf(t, err, "artifact store")
	// A resume sets the base's ending aside before it resets the run.
	endings, err := artifacts.NewRunArchive(s3Config())
	must.OKf(t, err, "run archive")
	archiver := engine.NewRunArchiver(endings)
	s.reader.WithEndings(archiver)
	seam := s.starter.WithConcurrencyIndex(true).WithClusterIdentity(s.namespace, "ork-resume-it")
	settings := api.DefaultResumeSettings()
	settings.ConfirmTimeout = 2 * time.Minute
	var clockMu sync.Mutex
	offset := time.Hour
	svc := api.NewService(api.Config{
		ResumeSettings: settings, Reader: s.reader, Starter: seam, Pools: servedPools{}, Commits: commits, ArtifactStore: store,
		Attempts: artifacts.NewMemoryAttemptStore(), Resume: seam, ResumeRecords: records,
		ResumeClaims: &resumeClaims{claims: map[string]engine.Claim{}}, ResumeSnapshots: s.snapshots,
		ResumeEndings: endings.(artifacts.RunEndingStore), ResumeArchiver: engine.NewHistoryArchiver(s.reader, archiver),
		Releases:          runningRelease{build: s.deployment.BuildID},
		ConcurrencyReader: engine.NewTemporalConcurrencyReader(s.client, 0), Capabilities: everyEngineHolds{},
		Now: func() time.Time {
			clockMu.Lock()
			defer clockMu.Unlock()
			return time.Now().Add(offset)
		},
	})
	// resume previews and confirms, and returns the resumed execution.
	resume := func() string {
		t.Helper()
		preview, err := svc.ResumeRun(ctx, connect.NewRequest(&orkstackv1.ResumeRunRequest{RunId: runID, DryRun: true}))
		must.OKf(t, err, "preview")
		made, err := svc.ResumeRun(ctx, connect.NewRequest(&orkstackv1.ResumeRunRequest{RunId: runID,
			ExpectedTerminalSequence: preview.Msg.GetTerminalSequence(), ExpectedPlanDigest: preview.Msg.GetPlanDigest()}))
		must.OKf(t, err, "confirm")
		if made.Msg.GetSeqBase() != preview.Msg.GetSeqBase() {
			t.Fatalf("resumed at %d, previewed %d", made.Msg.GetSeqBase(), preview.Msg.GetSeqBase())
		}
		d, err := describeExecution(s.client, ctx, runID, "")
		must.OKf(t, err, "describe the resumed run")
		clockMu.Lock()
		offset += time.Hour
		clockMu.Unlock()
		return d.GetWorkflowExecutionInfo().GetExecution().GetRunId()
	}

	// The first resume: the key is free, so it is granted, and deploy fails
	// again.
	first := resume()
	if err := s.client.GetWorkflow(ctx, runID, first).Get(ctx, nil); err == nil {
		t.Fatal("the first resume succeeded; deploy was meant to fail again")
	}
	// The base itself may have waited: the first run at a key does.
	if n := c.countEvents(runID, orkstackv1.EventType_EVENT_TYPE_RUN_QUEUED, "resumed, and waiting"); n != 0 {
		t.Fatalf("a resume at a free key waited %d times", n)
	}

	// The second: another run holds the key, so it waits, RUNNING.
	c.start(otherID, resumeTwoTaskDocument, plan)
	c.waitGate(gate, "the other run holding", func(v engine.GateStateView) bool { return slices.Equal(holders(v), []string{otherID}) })
	second := resume()
	v := c.waitGate(gate, "the resumed run in line", func(v engine.GateStateView) bool { return slices.Contains(waiting(v), runID) })
	if fault := gateFault(v); fault != "" {
		t.Fatal(fault)
	}
	c.waitFor("RUN_QUEUED", func() bool {
		return c.countEvents(runID, orkstackv1.EventType_EVENT_TYPE_RUN_QUEUED, "resumed, and waiting for key "+key) == 1
	})
	run, err := s.reader.GetRun(ctx, runID)
	must.OKf(t, err, "read the queued run")
	if run.GetStatus() != orkstackv1.Status_STATUS_RUNNING || run.GetQueued().GetCause() != orkstackv1.ConcurrencyQueueCause_CONCURRENCY_QUEUE_CAUSE_RESUME {
		t.Fatalf("queued run = %v, queued %v; want RUNNING, cause RESUME", run.GetStatus(), run.GetQueued())
	}
	close(release)
	c.mustEnd(otherID)
	must.OKf(t, s.client.GetWorkflow(ctx, runID, second).Get(ctx, nil), "the second resume")
	mu.Lock()
	if deploys != 3 {
		t.Errorf("deploy ran %d times, want once a history", deploys)
	}
	mu.Unlock()
	c.waitGate(gate, "nothing held or waiting", func(v engine.GateStateView) bool {
		return len(v.State.Holders) == 0 && len(v.State.Queue) == 0
	})

	// Its earlier incarnations were released at the gate in a workflow task
	// before the one that asked for its slot (CR-H2), at each resume. The
	// first is the committed fixture.
	reading := &gateStack{t: t, ctx: ctx, c: s.client}
	for name, execution := range map[string]string{"the first": first, "the second": second} {
		must.OKf(t, releasedBeforeItAsked(reading.history(t, runID, execution)), "%s resume", name)
	}
	h := reading.history(t, runID, first)
	must.OKf(t, replayConcurrencyRun(h, runID), "the resume just recorded does not replay")
	data := encodeHistory(t, h)
	proof.Write(t, "concurrency-resume", filepath.Base(concurrencyResumeFixture), data)
	if _, err := os.Stat(concurrencyResumeFixture); err != nil && os.Getenv("ORK_RECORD_GOLDEN_HISTORY") != "" {
		must.OKf(t, os.WriteFile(concurrencyResumeFixture, data, 0o644), "write %s", concurrencyResumeFixture)
	}
	var wire *connect.Error
	if _, err := svc.ResumeRun(ctx, connect.NewRequest(&orkstackv1.ResumeRunRequest{RunId: runID, DryRun: true})); !errors.As(err, &wire) {
		t.Fatalf("a run that succeeded can be resumed: %v", err)
	}
}
