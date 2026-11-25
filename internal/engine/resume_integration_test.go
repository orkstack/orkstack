//go:build integration

package engine_test

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/webhooktest"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// resumeAFailedRun runs build, which succeeds, and deploy, which fails, then
// resumes the run by a real reset of its closed execution (E85 PR4a): the
// reset's reason names a plan record that keeps build and runs deploy again.
// It returns once the resumed run has succeeded, with the idempotency keys
// dispatched over both.
func resumeAFailedRun(t *testing.T, ctx context.Context, label string) (stack *canStack, id string, resumeScope uint64, keys []string) {
	t.Helper()
	commits := artifacts.NewMemoryCommitStore()
	var mu sync.Mutex
	var dispatched []string
	behavior := func(actx context.Context, s *canStack, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		mu.Lock()
		dispatched = append(dispatched, in.GetIdempotencyKey())
		mu.Unlock()
		if in.GetTaskId() == "deploy" && !strings.Contains(in.GetIdempotencyKey(), "@r") {
			return nil, temporal.NewNonRetryableApplicationError("deploy failed", "ActivityFailed", nil)
		}
		commit := &orkstackv1.ActivityCommit{
			SchemaVersion: artifacts.CommitSchemaVersion, Namespace: artifacts.ResolveNamespace(in.GetNamespace()),
			RunId: in.GetRunId(), TaskId: in.GetTaskId(), OrkflowDefinitionDigest: in.GetOrkflowDefinitionDigest(),
			IdempotencyKey: in.GetIdempotencyKey(), Image: in.GetImage(), Attempt: 1, OrkerId: "orker-it",
		}
		record, err := commits.Create(actx, in.GetCommitUri(), commit)
		if err != nil {
			return nil, err
		}
		result := succeeded(in.GetTaskId(), map[string]any{"ok": true})
		result.Commit = &orkstackv1.CommitRef{Uri: in.GetCommitUri(), Sha256: record.SHA256}
		return result, nil
	}
	records, err := artifacts.NewRecordStore(ctx, s3Config())
	must.OKf(t, err, "record store")
	var s *canStack
	s = startCANStack(t, ctx, label, false, behavior, func(w worker.Worker) {
		acts := engine.NewResumeActivities(commits, nil).WithHistory(lazyHistory{&s}, lazyNamespace(&s), records)
		w.RegisterActivityWithOptions(acts.ReadResetFacts, activity.RegisterOptions{Name: engine.ActivityReadResetFacts})
		w.RegisterActivityWithOptions(acts.LoadResumePlan, activity.RegisterOptions{Name: engine.ActivityLoadResumePlan})
		w.RegisterActivityWithOptions(acts.AdoptRecorded, activity.RegisterOptions{Name: engine.ActivityAdoptRecorded})
	})

	// The base: build succeeds, deploy fails.
	runID := fmt.Sprintf("ork-resume-it-%d", time.Now().UnixNano())
	plan := s.plan(10_000, nil)
	must.OKf(t, s.starter.StartRun(ctx, runID, compile(t, resumeTwoTaskDocument), nil, plan), "StartRun")
	base := s.client.GetWorkflow(ctx, runID, "")
	if err := base.Get(ctx, nil); err == nil {
		t.Fatal("the base run succeeded; deploy was meant to fail")
	}
	baseExecution := base.GetRunID()
	recorded := s.history(runID, baseExecution)
	scanned, err := engine.ScanResumeBase(recorded.GetEvents(), converter.GetDefaultDataConverter())
	must.OKf(t, err, "scan the base")
	var terminal int64
	for _, e := range s.bus.Events(runID, 0) {
		terminal = max(terminal, e.GetSequence())
	}
	if terminal < 3 {
		t.Fatalf("the base published up to sequence %d", terminal)
	}

	// The plan ResumeRun would record, and the identity record that makes it
	// the newest resume.
	scope := uint64(terminal + 1)
	record := &orkstackv1.ResumePlanRecord{
		Format: artifacts.ResumePlanFormat, Namespace: "default", RunId: runID, Scope: scope, N: 0,
		BaseExecutionRunId: baseExecution,
		RScheduledEventId:  scanned.ResetPoint.ScheduledEventID, RStartedEventId: scanned.ResetPoint.StartedEventID,
		BaseTerminalSequence: terminal, Principal: "ops@example.com", Reason: "integration",
		Block: &orkstackv1.ResumeBlock{
			SeqBase: scope, Scope: scope, Action: orkstackv1.ResumeAction_RESUME_ACTION_RESUME,
			Readback: &orkstackv1.ResumeReadback{
				Deadline: durationpb.New(2 * time.Minute), Budget: 128 << 10, MaxEntries: 128, Concurrency: 4,
				GetTimeout: durationpb.New(10 * time.Second), GetTries: 3, CallTimeout: durationpb.New(30 * time.Second),
				RetryInitial: durationpb.New(time.Second), RetryCoefficient: 2, CallsPerTask: 4,
			},
		},
	}
	must.OKf(t, engine.EncodeResumeUnits(compile(t, resumeTwoTaskDocument), record.Block,
		[]engine.ResumeUnitPlan{keptUnit("build"), rerunUnit("deploy")}, false), "pack the units")
	record.PlanDigest = engine.ResumePlanDigestOf(record)
	digest, err := records.WriteResumePlan(ctx, record)
	must.OKf(t, err, "write the plan")
	must.OKf(t, records.WriteResume(ctx, artifacts.ResumeRecord{Namespace: "default", RunID: runID, Scope: scope, N: 0, Tasks: []string{"deploy"}}),
		"write the identity record")

	// The one reset, with the plan key as its reason.
	key := engine.ResumePlanKey{Scope: scope, N: 0, PlanDigest: digest}
	reset, err := s.client.WorkflowService().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 s.namespace,
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: runID, RunId: baseExecution},
		Reason:                    key.String(),
		WorkflowTaskFinishEventId: scanned.ResetPoint.CompletedEventID,
		RequestId:                 uuid.NewString(),
		// Nothing the base received after R is applied again.
		ResetReapplyExcludeTypes: []enumspb.ResetReapplyExcludeType{
			enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL,
			enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE,
			enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS,
		},
	})
	must.OKf(t, err, "reset the base to R")
	if err := s.client.GetWorkflow(ctx, runID, reset.GetRunId()).Get(ctx, nil); err != nil {
		t.Fatalf("the resumed run failed: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	return s, runID, scope, slices.Clone(dispatched)
}

// A failed run resumed by a real reset of its closed execution (E85 PR4a):
// the reset's reason names a plan record; the reset run finds its reset in
// its own history, loads and verifies the record, keeps build from its
// commit without running it, and runs deploy again under the resume's key.
func TestIntegration_AResumedRunKeepsWhatSucceededAndRunsTheRestAgain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	s, runID, scope, dispatched := resumeAFailedRun(t, ctx, "resume")
	var builds, reruns int
	for _, k := range dispatched {
		switch {
		case strings.HasSuffix(k, "/build"):
			builds++
		case strings.HasSuffix(k, fmt.Sprintf("/deploy@r%d", scope)):
			reruns++
		}
	}
	if builds != 1 || reruns != 1 {
		t.Fatalf("dispatched %v: want build once (the base only) and deploy once under scope %d", dispatched, scope)
	}
	var resumed bool
	for _, e := range s.bus.Events(runID, 0) {
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_RUN_RESUMED {
			resumed = e.GetSequence() == int64(scope) && e.GetResumed().GetPrincipal() == "ops@example.com"
		}
	}
	if !resumed {
		t.Fatalf("no RUN_RESUMED at %d", scope)
	}
}

// TestIntegration_WebhookRunResumed is rule W5's test for RUN_RESUMED: one
// delivery to the subscription naming it, RUNNING, with its sequence and
// resumeCount, and one to the subscription with no events, after that run's
// RUN_FAILED and before its RUN_SUCCEEDED.
func TestIntegration_WebhookRunResumed(t *testing.T) {
	resumedWebhook(t, webhooks.EventRunResumed, func(t *testing.T, body resumeWebhookBody, scope uint64) {
		if body.ResumeCount != 1 || body.ResultsKept != nil || body.Sequence != int64(scope) {
			t.Errorf("RUN_RESUMED body = %+v, want resumeCount 1 at sequence %d", body, scope)
		}
	})
}

// TestIntegration_WebhookRunResultsKept is rule W5's test for
// RUN_RESULTS_KEPT: one delivery to the subscription naming it, RUNNING, with
// what was kept, and none to the subscription with no events.
func TestIntegration_WebhookRunResultsKept(t *testing.T) {
	resumedWebhook(t, webhooks.EventRunResultsKept, func(t *testing.T, body resumeWebhookBody, scope uint64) {
		if body.ResultsKept == nil || body.ResultsKept.KeptCount != 1 || body.ResumeCount != 0 || body.Sequence != int64(scope)+1 {
			t.Errorf("RUN_RESULTS_KEPT body = %+v, want keptCount 1 at sequence %d", body, scope+1)
		}
	})
}

type resumeWebhookBody struct {
	Sequence    int64                 `json:"sequence"`
	ResumeCount int32                 `json:"resumeCount"`
	ResultsKept *webhooks.ResultsKept `json:"resultsKept"`
}

// resumedWebhook resumes a failed run with two subscriptions, one naming
// event and one naming none, and checks what each received.
func resumedWebhook(t *testing.T, event webhooks.Event, check func(*testing.T, resumeWebhookBody, uint64)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	subscriber := newWebhookSubscriber(t)
	registry := webhooks.NewRegistry(webhooks.WithDestinationPolicy(webhooktest.LoopbackPolicy()))
	notifier := webhooks.NewNotifier(registry, webhooks.NewDispatcher(webhooks.DispatcherOptions{Policy: webhooktest.LoopbackPolicy()}),
		webhooks.NotifierOptions{})
	notifier.Observe(subscriber.record)
	named, err := registry.Register(webhooks.Registration{URL: subscriber.server.URL, Events: []webhooks.Event{event}})
	must.OKf(t, err, "register the %s webhook", event)
	plain, err := registry.Register(webhooks.Registration{URL: subscriber.server.URL})
	must.OKf(t, err, "register the webhook with no events")

	canStackRunNotifier = api.NewWebhookNotifier(notifier, nil)
	defer func() { canStackRunNotifier = nil }()
	_, runID, scope, _ := resumeAFailedRun(t, ctx, "resume-hook")

	// The run's own events are what must be delivered, and no more. The plain
	// subscription's second ending is the progress that makes an absence mean
	// something.
	wantPlain := []string{"RUN_STARTED", "RUN_FAILED", "RUN_RESUMED", "RUN_SUCCEEDED"}
	of := func(hook string) (events []string, bodies [][]byte, statuses []string) {
		type delivery struct {
			event, status string
			body          []byte
			seq           int64
		}
		var got []delivery
		for _, d := range subscriber.deliveries(runID) {
			if d.webhookID != hook {
				continue
			}
			var b resumeWebhookBody
			must.OKf(t, json.Unmarshal(d.body, &b), "decode %s", d.event)
			got = append(got, delivery{d.event, d.payload.Status, d.body, b.Sequence})
		}
		slices.SortFunc(got, func(a, b delivery) int { return int(a.seq - b.seq) })
		for _, d := range got {
			events, bodies, statuses = append(events, d.event), append(bodies, d.body), append(statuses, d.status)
		}
		return events, bodies, statuses
	}
	waitForWebhooks(t, ctx, "the deliveries to arrive", func() bool {
		a, _, _ := of(named.ID)
		b, _, _ := of(plain.ID)
		return len(a) >= 1 && len(b) >= len(wantPlain)
	})
	drain, drainCancel := context.WithTimeout(ctx, 30*time.Second)
	defer drainCancel()
	must.OKf(t, notifier.Wait(drain), "deliveries did not settle")

	if got, _, _ := of(plain.ID); !slices.Equal(got, wantPlain) {
		t.Fatalf("the webhook with no events got %v, want %v", got, wantPlain)
	}
	events, bodies, statuses := of(named.ID)
	if len(events) != 1 || events[0] != string(event) || statuses[0] != "RUNNING" {
		t.Fatalf("the %s webhook got %v with statuses %v, want one, RUNNING", event, events, statuses)
	}
	var body resumeWebhookBody
	must.OKf(t, json.Unmarshal(bodies[0], &body), "decode")
	check(t, body, scope)
	t.Logf("EVIDENCE webhook_%s run=%s sequence=%d", strings.ToLower(string(event)), runID, body.Sequence)
}

func waitForWebhooks(t *testing.T, ctx context.Context, what string, ok func() bool) {
	t.Helper()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for !ok() {
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for %s: %v", what, ctx.Err())
		case <-tick.C:
		}
	}
}

// lazyHistory reads through the stack's client once the stack exists: the
// run worker's activities are registered while the stack is being built.
type lazyHistory struct{ s **canStack }

func (l lazyHistory) GetWorkflowExecutionHistory(ctx context.Context, in *workflowservice.GetWorkflowExecutionHistoryRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	in.Namespace = (*l.s).namespace
	return (*l.s).client.WorkflowService().GetWorkflowExecutionHistory(ctx, in, opts...)
}

func lazyNamespace(**canStack) string { return "" }
