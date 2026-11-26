//go:build integration

package engine_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	archivecleanup "github.com/orkstack/orkstack/internal/archive"
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
	// What the reset sets aside is read from the archive of the ending before.
	endings, err := artifacts.NewRunArchive(s3Config())
	must.OKf(t, err, "run archive")
	archiver := engine.NewRunArchiver(endings)
	s.reader.WithEndings(archiver)
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
		ResumeEndings: endings.(artifacts.RunEndingStore), ResumeArchiver: engine.NewHistoryArchiver(s.reader, archiver),
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
	checkResumedRunArchived(t, ctx, s, endings, archiver, runID, baseExecution, preview)

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

// checkResumedRunArchived is a resumed run that succeeded, from its end to its
// retirement: its stream is whole, the sweep archives its second ending beside
// its first and once, every list has it once as it ended last, and deleting it
// deletes every execution of it.
func checkResumedRunArchived(t *testing.T, ctx context.Context, s *canStack, endings artifacts.RunArchive,
	archiver *engine.RunArchiver, runID, baseExecution string, resumed *orkstackv1.ResumeRunResponse,
) {
	t.Helper()
	numbered := func(events []*orkstackv1.Event) []string {
		var out []string
		for _, e := range events {
			out = append(out, fmt.Sprintf("%d:%s", e.GetSequence(), e.GetType()))
		}
		return out
	}
	live, err := s.reader.Events(ctx, runID, 0)
	must.OKf(t, err, "the live stream")
	for i, e := range live {
		if e.GetSequence() != int64(i+1) {
			t.Fatalf("the live stream is not whole: %v", numbered(live))
		}
	}
	terminal, base := resumed.GetTerminalSequence(), resumed.GetSeqBase()
	if live[terminal-1].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED || live[base-1].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_RESUMED ||
		live[len(live)-1].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED {
		t.Fatalf("the live stream = %v, want the failure at %d, the resume at %d and a success", numbered(live), terminal, base)
	}

	store := endings.(artifacts.RunEndingStore)
	newest := archivecleanup.NewNewestEndings(store, s.reader, s.namespace)
	if has, err := newest.HasRun(ctx, runID); err != nil || has {
		t.Fatalf("archived before the sweep = %v, %v; only the ending set aside is", has, err)
	}
	sweeper := archivecleanup.NewSweeper(archivecleanup.CleanupConfig{TemporalNamespace: s.namespace, Interval: time.Hour, BatchSize: 10},
		s.reader, endings, engine.NewHistoryArchiver(s.reader, archiver), s.reader, nil, slog.New(slog.NewTextHandler(io.Discard, nil))).
		WithEndings(store, newest).WithClock(func() time.Time { return time.Now().Add(2 * time.Hour) })
	sweepUntil(t, ctx, sweeper, func(r archivecleanup.SweepResult) bool { return r.Archived == 1 })
	if again, err := sweeper.RunOnce(ctx); err != nil || again.Archived != 0 {
		t.Fatalf("a second pass = %+v, %v; want nothing more archived", again, err)
	}
	if has, err := newest.HasRun(ctx, runID); err != nil || !has {
		t.Fatalf("archived after the sweep = %v, %v", has, err)
	}
	body, err := store.GetEnding(ctx, runID, uint64(base))
	must.OKf(t, err, "the second ending's body")
	run, archived, err := engine.UnmarshalRunArchive(body)
	must.OKf(t, err, "decode it")
	gaps, err := engine.UnmarshalRunArchiveGaps(body)
	if err != nil || len(gaps) != 0 || run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || fmt.Sprint(numbered(archived)) != fmt.Sprint(numbered(live)) {
		t.Fatalf("archived %v %v, gaps %v, %v; want the live stream %v", run.GetStatus(), numbered(archived), gaps, err, numbered(live))
	}
	first, err := store.GetEnding(ctx, runID, 0)
	must.OKf(t, err, "the first ending's body")
	if run, events, err := engine.UnmarshalRunArchive(first); err != nil || run.GetStatus() != orkstackv1.Status_STATUS_FAILED || int64(len(events)) != terminal {
		t.Fatalf("the first ending = %v with %d events, %v; want FAILED with %d", run.GetStatus(), len(events), err, terminal)
	}
	for status, want := range map[orkstackv1.Status]int{orkstackv1.Status_STATUS_FAILED: 0, orkstackv1.Status_STATUS_SUCCEEDED: 1} {
		if runs, err := archiver.ListRuns(ctx, engine.ArchiveQuery{Namespace: "default", Status: status}); err != nil || len(runs) != want {
			t.Errorf("the archive lists %d %v runs, %v; want %d", len(runs), status, err, want)
		}
	}

	must.OKf(t, s.reader.DeleteWorkflow(ctx, s.namespace, runID), "DeleteWorkflow")
	awaitWorkflowGone(t, ctx, s.reader, s.namespace, runID)
	for {
		_, err := s.client.DescribeWorkflow(ctx, runID, baseExecution)
		var gone *serviceerror.NotFound
		if errors.As(err, &gone) {
			break
		}
		must.OKf(t, ctx.Err(), "the execution the resume set aside was not deleted (%v)", err)
		select {
		case <-ctx.Done():
		case <-time.After(100 * time.Millisecond):
		}
	}
	if has, err := newest.HasRun(ctx, runID); err != nil || !has {
		t.Errorf("with the cluster's copy gone, archived = %v, %v", has, err)
	}
	if after, err := archivecleanup.NewEndingRecords(store).ResumedAfter(ctx, runID, time.Time{}); err != nil || after {
		t.Errorf("resumed after its newest archived ending = %v, %v", after, err)
	}
	if run, err := archiver.LoadRun(ctx, runID); err != nil || run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Errorf("with the cluster's copy gone the run reads %v, %v", run.GetStatus(), err)
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
