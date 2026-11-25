//go:build integration

package engine_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// heldResets is the cluster with its first reset held back: its answer reads
// as lost, and it is sent only when the test delivers it. It stands for a
// request the network delayed, which no real cluster does on demand.
type heldResets struct {
	engine.ResumeCluster
	mu   sync.Mutex
	held []engine.ResumeReset
	sent int
}

func (h *heldResets) ResetForResume(ctx context.Context, reset engine.ResumeReset) (string, error) {
	h.mu.Lock()
	if len(h.held) == 0 {
		h.held = append(h.held, reset)
		h.mu.Unlock()
		return "", engine.ErrResetOutcomeUnknown
	}
	h.sent++
	h.mu.Unlock()
	return h.ResumeCluster.ResetForResume(ctx, reset)
}

// first is the reset held back, and others counts those sent since.
func (h *heldResets) first() engine.ResumeReset {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.held[0]
}

func (h *heldResets) others() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sent
}

// resumeClaims are claims in memory, with a hook before each.
type resumeClaims struct {
	mu      sync.Mutex
	claims  map[string]engine.Claim
	onClaim func()
}

func (c *resumeClaims) Lookup(_ context.Context, key string) (engine.Claim, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	held, ok := c.claims[key]
	return held, ok, nil
}

func (c *resumeClaims) Claim(_ context.Context, key string, candidate engine.Claim) (engine.Claim, error) {
	if c.onClaim != nil {
		c.onClaim()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if held, ok := c.claims[key]; ok {
		return held, nil
	}
	c.claims[key] = candidate
	return candidate, nil
}

// servedPools is every pool served: whether one is decides nothing here. It
// says no time an Orker was seen at, which the service would compare with the
// clock the test moves.
type servedPools struct{}

func (servedPools) ObservePool(context.Context, string) (engine.PoolLiveness, error) {
	return engine.PoolLiveness{Orkers: 1}, nil
}

// ResumeRun against a real cluster, on a versioned deployment: a run that
// failed is resumed under its own id; the first sending of its reset is lost
// and lands just as the second is prepared, and the run is reset exactly once.
func TestIntegration_ResumeRunResetsOnceHoweverItsSendsInterleave(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
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
	var s *canStack
	s = startCANStack(t, ctx, "resume-run", true, behavior, func(w worker.Worker) {
		acts := engine.NewResumeActivities(commits, nil).WithHistory(lazyHistory{&s}, lazyNamespace(&s), records)
		w.RegisterActivityWithOptions(acts.ReadResetFacts, activity.RegisterOptions{Name: engine.ActivityReadResetFacts})
		w.RegisterActivityWithOptions(acts.LoadResumePlan, activity.RegisterOptions{Name: engine.ActivityLoadResumePlan})
		w.RegisterActivityWithOptions(acts.AdoptRecorded, activity.RegisterOptions{Name: engine.ActivityAdoptRecorded})
	})
	runID := fmt.Sprintf("ork-resume-run-%d", time.Now().UnixNano())
	must.OKf(t, s.starter.StartRun(ctx, runID, compile(t, resumeTwoTaskDocument), nil, s.plan(10_000, nil)), "StartRun")
	base := s.client.GetWorkflow(ctx, runID, "")
	if err := base.Get(ctx, nil); err == nil {
		t.Fatal("the base run succeeded; deploy was meant to fail")
	}
	baseExecution := base.GetRunID()

	store, err := artifacts.NewLocalStore(t.TempDir())
	must.OKf(t, err, "artifact store")
	seam := s.starter.WithClusterIdentity(s.namespace, "ork-resume-it")
	cluster := &heldResets{ResumeCluster: seam}
	claims := &resumeClaims{claims: map[string]engine.Claim{}}
	var clockMu sync.Mutex
	// Past everything the base may still be running.
	offset := time.Hour
	// The wait for the run to carry on has room for a slow host, and the
	// window of a send is well within the ten minutes the clock is moved by.
	settings := api.DefaultResumeSettings()
	settings.ConfirmTimeout = 2 * time.Minute
	svc := api.NewService(api.Config{
		ResumeSettings: settings,
		Reader:         s.reader, Starter: s.starter, Pools: servedPools{}, Commits: commits, ArtifactStore: store,
		Attempts: artifacts.NewMemoryAttemptStore(), Resume: cluster, ResumeRecords: records, ResumeClaims: claims, ResumeSnapshots: s.snapshots,
		Releases: runningRelease{build: s.deployment.BuildID},
		Now: func() time.Time {
			clockMu.Lock()
			defer clockMu.Unlock()
			return time.Now().Add(offset)
		},
	})
	resume := func(req *orkstackv1.ResumeRunRequest) (*orkstackv1.ResumeRunResponse, *orkstackv1.ResumeRefusal) {
		t.Helper()
		req.RunId = runID
		resp, err := svc.ResumeRun(ctx, connect.NewRequest(req))
		if err == nil {
			return resp.Msg, nil
		}
		var wire *connect.Error
		if errors.As(err, &wire) {
			for _, d := range wire.Details() {
				if v, derr := d.Value(); derr == nil {
					if r, ok := v.(*orkstackv1.ResumeRefusal); ok {
						return nil, r
					}
				}
			}
		}
		t.Fatalf("ResumeRun = %v", err)
		return nil, nil
	}
	preview, refused := resume(&orkstackv1.ResumeRunRequest{DryRun: true})
	if refused != nil || preview.GetKeptCount() != 1 || len(preview.GetTasks()) != 2 {
		t.Fatalf("preview = %v, refused %v", preview, refused)
	}
	confirm := func() (*orkstackv1.ResumeRunResponse, *orkstackv1.ResumeRefusal) {
		t.Helper()
		return resume(&orkstackv1.ResumeRunRequest{ExpectedTerminalSequence: preview.GetTerminalSequence(), ExpectedPlanDigest: preview.GetPlanDigest()})
	}
	if _, r := confirm(); r.GetReason() != "RESUME_UNCONFIRMED" || r.GetCause() != "reset_outcome_unknown" {
		t.Fatalf("a lost reset = %v", r)
	}
	if _, r := confirm(); r.GetReason() != "RESUME_IN_PROGRESS" {
		t.Fatalf("at once = %v, want RESUME_IN_PROGRESS", r)
	}
	clockMu.Lock()
	offset += 10 * time.Minute
	clockMu.Unlock()
	// The first sending lands as the second is prepared.
	var landed string
	claims.onClaim = func() {
		id, err := seam.ResetForResume(ctx, cluster.first())
		must.OKf(t, err, "deliver the first sending")
		landed = id
	}
	if _, r := confirm(); r.GetReason() != "RUN_CHANGED" {
		t.Fatalf("the second = %v, want RUN_CHANGED", r)
	}
	claims.onClaim = nil
	made, r := confirm()
	if r != nil || !made.GetAlreadyResumed() || made.GetSeqBase() != preview.GetSeqBase() {
		t.Fatalf("again = %v, refused %v; want the resume already made", made, r)
	}
	if n := cluster.others(); n != 0 {
		t.Fatalf("%d more resets were sent, want none", n)
	}
	if err := s.client.GetWorkflow(ctx, runID, landed).Get(ctx, nil); err != nil {
		t.Fatalf("the resumed run failed: %v", err)
	}
	after, err := seam.DescribeResumeExecution(ctx, runID, baseExecution)
	must.OKf(t, err, "describe the base")
	current, err := seam.DescribeResumeExecution(ctx, runID, "")
	must.OKf(t, err, "describe the run")
	if after.ResetRunID != landed || current.ExecutionRunID != landed || current.BuildID != s.deployment.BuildID || after.BuildID != s.deployment.BuildID {
		t.Fatalf("base %+v, current %+v; want the one reset %s, both on build %s", after, current, landed, s.deployment.BuildID)
	}
	mu.Lock()
	defer mu.Unlock()
	reruns := 0
	for _, k := range dispatched {
		if strings.HasSuffix(k, fmt.Sprintf("/deploy@r%d", preview.GetSeqBase())) {
			reruns++
		}
	}
	if reruns != 1 {
		t.Fatalf("dispatched %v, want deploy run again once", dispatched)
	}
}

// runningRelease is the cluster's releases with one build's engines running.
type runningRelease struct {
	engine.ReleaseCluster
	build string
}

func (r runningRelease) ObserveReleases(context.Context) (engine.ReleaseObservation, error) {
	return engine.ReleaseObservation{EngineReplicas: map[string]int{r.build: 1}}, nil
}
