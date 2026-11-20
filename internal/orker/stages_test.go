package orker

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// failureDetailOf reads the ActivityFailureDetail an Orker's report carries.
func failureDetailOf(t *testing.T, err error) *orkstackv1.ActivityFailureDetail {
	t.Helper()
	var app *temporal.ApplicationError
	if !errors.As(err, &app) {
		t.Fatalf("%v is not an application error", err)
	}
	var detail *orkstackv1.ActivityFailureDetail
	must.OKf(t, app.Details(&detail), "read the failure detail")
	return detail
}

// listsInterruptions makes d's attempts carry the started/never_started
// fields in their failure detail, as an attempt of a run that pinned the
// interruption class does (an engine that lists the cluster's heartbeat
// timeout among its retry policy's non-retryable types).
func listsInterruptions(d *OCIDispatcher) {
	d.listed = func(context.Context) []string { return []string{HeartbeatTimeoutType} }
}

// sweepOnStartReturnRuntime is a held container whose Start runs the stop's
// sweep just before it returns success: the sweep lands between the start's
// return and the attempt's CAS to revocable. Hand-written because a real
// daemon cannot be made to return from a start at a chosen instant
// (AGENTS.md section 11.1, step 3).
type sweepOnStartReturnRuntime struct {
	*heldRuntime
	sweep func()
}

func (r *sweepOnStartReturnRuntime) Start(ctx context.Context, id ContainerID, stdout, stderr io.Writer) error {
	err := r.heldRuntime.Start(ctx, id, stdout, stderr)
	r.sweep()
	return err
}

// TestStages_SweepBetweenStartReturnAndCAS proves that a sweep landing after
// the container's start returned and before the CAS to revocable does not
// make the attempt a never-started hand-back: the container ran, so the
// ordinary path takes over (the cancellation sequence, SIGTERM first) and the
// report is ORKER_STOPPED with started=true and never_started=false.
func TestStages_SweepBetweenStartReturnAndCAS(t *testing.T) {
	r := newStopRegistry()
	stage := r.admit()
	defer r.leave(stage)
	runtime := &sweepOnStartReturnRuntime{heldRuntime: newHeldRuntime(), sweep: func() { r.handBack() }}
	d, reports := stoppingDispatcher(t, runtime, artifacts.NewMemoryAttemptStore())
	listsInterruptions(d)
	in := baseExportInput("sweep-before-cas")

	result, err := d.Execute(withStage(context.Background(), stage), in)
	if !errors.Is(err, activity.ErrResultPending) || result != nil {
		t.Fatalf("Execute = (%v, %v), want (nil, ErrResultPending) once the Orker reported it", result, err)
	}
	if !stage.handedBack() {
		t.Fatal("the sweep did not take the attempt back; the test proves nothing")
	}
	if runtime.first != SIGTERM {
		t.Errorf("the container's first signal = %v, want SIGTERM (the cancellation sequence)", runtime.first)
	}
	if len(reports.reports) != 1 || !reportedReason(reports.reports[0], ReasonOrkerStopped) {
		t.Fatalf("reports = %v, want one retryable ORKER_STOPPED", reports.reports)
	}
	detail := failureDetailOf(t, reports.reports[0])
	if !detail.GetStarted() || detail.GetNeverStarted() {
		t.Errorf("detail started=%v never_started=%v, want started=true never_started=false",
			detail.GetStarted(), detail.GetNeverStarted())
	}
}

// sweepAfterCreateRuntime sweeps once the container is created and counts
// Start and Remove calls.
type sweepAfterCreateRuntime struct {
	phaseCapturingRuntime
	sweep   func()
	starts  atomic.Int32
	mu      sync.Mutex
	created []ContainerID
	removes map[ContainerID]int
}

func (r *sweepAfterCreateRuntime) Create(ctx context.Context, spec ContainerSpec) (ContainerID, error) {
	id, err := r.phaseCapturingRuntime.Create(ctx, spec)
	if err != nil {
		return id, err
	}
	r.mu.Lock()
	r.created = append(r.created, id)
	r.mu.Unlock()
	r.sweep()
	return id, nil
}

func (r *sweepAfterCreateRuntime) Start(context.Context, ContainerID, io.Writer, io.Writer) error {
	r.starts.Add(1)
	return nil
}

func (r *sweepAfterCreateRuntime) Remove(_ context.Context, id ContainerID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.removes == nil {
		r.removes = map[ContainerID]int{}
	}
	r.removes[id]++
	return nil
}

// TestStages_AdmittedSweptAfterCreateRemovesContainer proves that an attempt
// the sweep takes back after its container was created and before it started
// never starts it: the created container is removed, the report is
// ORKER_STOPPED with never_started=true and started=false, and nothing is
// committed.
func TestStages_AdmittedSweptAfterCreateRemovesContainer(t *testing.T) {
	r := newStopRegistry()
	stage := r.admit()
	defer r.leave(stage)
	runtime := &sweepAfterCreateRuntime{sweep: func() { r.handBack() }}
	commits := artifacts.NewMemoryCommitStore()
	d, reports := stoppingDispatcher(t, runtime, artifacts.NewMemoryAttemptStore())
	d.Commits = commits
	listsInterruptions(d)
	in := baseExportInput("swept-after-create")
	in.CommitUri = "mem://commits/" + in.GetRunId() + "/swept-after-create/commit.json"

	_, err := d.Execute(withStage(context.Background(), stage), in)
	if !errors.Is(err, activity.ErrResultPending) {
		t.Fatalf("Execute = %v, want ErrResultPending once the Orker reported it", err)
	}
	if len(runtime.created) != 1 {
		t.Fatalf("created %d containers, want 1; the test proves nothing", len(runtime.created))
	}
	if got := runtime.starts.Load(); got != 0 {
		t.Errorf("Start was called %d times on an attempt the sweep took back before it started", got)
	}
	runtime.mu.Lock()
	removed := runtime.removes[runtime.created[0]]
	runtime.mu.Unlock()
	if removed < 1 {
		t.Errorf("the created container was removed %d times, want at least once", removed)
	}
	if len(reports.reports) != 1 || !reportedReason(reports.reports[0], ReasonOrkerStopped) {
		t.Fatalf("reports = %v, want one retryable ORKER_STOPPED", reports.reports)
	}
	detail := failureDetailOf(t, reports.reports[0])
	if !detail.GetNeverStarted() || detail.GetStarted() {
		t.Errorf("detail never_started=%v started=%v, want never_started=true started=false",
			detail.GetNeverStarted(), detail.GetStarted())
	}
	if _, err := commits.Get(context.Background(), in.GetCommitUri()); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("a commit exists for an attempt that never started (Get = %v)", err)
	}
}

// sweepingMemoStore runs sweep after its inner store answered a read, before
// the answer reaches the dispatcher: the sweep lands inside the memo lookup.
type sweepingMemoStore struct {
	artifacts.MemoStore
	sweep func()
}

func (s *sweepingMemoStore) GetMemo(ctx context.Context, ref artifacts.MemoRef) (*orkstackv1.CacheMemo, error) {
	memo, err := s.MemoStore.GetMemo(ctx, ref)
	if err == nil && s.sweep != nil {
		s.sweep()
	}
	return memo, err
}

// sweepingCommitStore runs sweep as its Create begins, which is after the
// attempt's CAS to committing and before the commit is written.
type sweepingCommitStore struct {
	artifacts.CommitStore
	sweep func()
}

func (s *sweepingCommitStore) Create(ctx context.Context, uri string, c *orkstackv1.ActivityCommit) (artifacts.CommitRecord, error) {
	if s.sweep != nil {
		s.sweep()
	}
	return s.CommitStore.Create(ctx, uri, c)
}

// uncancellableCommitStore writes whatever it is asked to, cancelled context
// or not, so that a test of the commit CAS cannot pass merely because the
// sweep's cancellation made a write fail.
type uncancellableCommitStore struct{ artifacts.CommitStore }

func (s uncancellableCommitStore) Create(ctx context.Context, uri string, c *orkstackv1.ActivityCommit) (artifacts.CommitRecord, error) {
	return s.CommitStore.Create(context.WithoutCancel(ctx), uri, c)
}

// TestStages_MemoHitWriteRacesTheSweep proves the commit CAS loser rule (R8-4):
// a memo hit whose lookup the sweep wins writes no commit.json and creates no
// container, and is a never-started hand-back; the positive control, a sweep
// after the CAS, writes the hit's commit, completes, and is not handed back.
func TestStages_MemoHitWriteRacesTheSweep(t *testing.T) {
	prepare := func(t *testing.T) (*cacheHarness, *attemptStage, *stopRegistry, *orkstackv1.ExecuteTaskInput) {
		t.Helper()
		h := newCacheHarness(t)
		listsInterruptions(h.dispatcher)
		h.produce(t, cachedTaskInput(t, "ork-stage-source"))
		r := newStopRegistry()
		stage := r.admit()
		t.Cleanup(func() { r.leave(stage) })
		return h, stage, r, cachedTaskInput(t, "ork-stage-hit")
	}

	t.Run("the sweep wins the lookup", func(t *testing.T) {
		h, stage, r, in := prepare(t)
		h.dispatcher.Memos = &sweepingMemoStore{MemoStore: h.memos, sweep: func() { r.handBack() }}
		// A store that writes despite the sweep's cancellation: only the CAS
		// can keep the commit from being written.
		h.dispatcher.Commits = uncancellableCommitStore{CommitStore: h.commits}
		_, createsBefore := h.runtime.counts()

		result, err := h.dispatcher.Execute(withStage(context.Background(), stage), in)
		if result != nil {
			t.Fatalf("Execute returned a result %v for an attempt the sweep took back", result)
		}
		if !stage.handedBack() {
			t.Fatal("the sweep did not take the attempt back; the test proves nothing")
		}
		if !reportedReason(err, ReasonOrkerStopped) {
			t.Fatalf("Execute = %v, want a retryable ORKER_STOPPED", err)
		}
		if detail := failureDetailOf(t, err); !detail.GetNeverStarted() || detail.GetStarted() {
			t.Errorf("detail never_started=%v started=%v, want never_started=true started=false",
				detail.GetNeverStarted(), detail.GetStarted())
		}
		if _, err := h.commits.MemoryCommitStore.Get(context.Background(), in.GetCommitUri()); !errors.Is(err, artifacts.ErrNotFound) {
			t.Errorf("a commit.json exists for an attempt the sweep took first (Get = %v)", err)
		}
		for _, uri := range h.commits.createdAt {
			if uri == in.GetCommitUri() {
				t.Errorf("Create was called for %s", uri)
			}
		}
		if _, creates := h.runtime.counts(); creates != createsBefore {
			t.Errorf("containers created %d -> %d, want none", createsBefore, creates)
		}
	})

	t.Run("the sweep after the CAS", func(t *testing.T) {
		h, stage, r, in := prepare(t)
		h.dispatcher.Commits = &sweepingCommitStore{CommitStore: h.commits, sweep: func() { r.handBack() }}
		_, createsBefore := h.runtime.counts()

		result, err := h.dispatcher.Execute(withStage(context.Background(), stage), in)
		must.OKf(t, err, "Execute")
		if result.GetCache().GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT {
			t.Fatalf("cache = %v, want HIT", result.GetCache())
		}
		if stage.handedBack() {
			t.Error("the sweep took a committing attempt back")
		}
		if _, err := h.commits.MemoryCommitStore.Get(context.Background(), in.GetCommitUri()); err != nil {
			t.Errorf("the hit's commit was not written: %v", err)
		}
		if _, creates := h.runtime.counts(); creates != createsBefore {
			t.Errorf("a hit created %d containers", creates-createsBefore)
		}
	})
}

// claimHookStore runs after fires on each ordinal claim: the first write an
// adopting attempt makes to the attempt store, which is after its CAS to
// committing.
type claimHookStore struct {
	artifacts.AttemptClaimer
	after func()
}

func (s *claimHookStore) ClaimAttempt(ctx context.Context, scope artifacts.AttemptRef, orkerID string) (artifacts.AttemptClaimed, error) {
	claimed, err := s.AttemptClaimer.ClaimAttempt(ctx, scope, orkerID)
	if s.after != nil {
		s.after()
	}
	return claimed, err
}

// getHookCommitStore runs before on each read, before delegating.
type getHookCommitStore struct {
	artifacts.CommitStore
	before func()
}

func (s *getHookCommitStore) Get(ctx context.Context, uri string) (artifacts.CommitRecord, error) {
	if s.before != nil {
		s.before()
	}
	return s.CommitStore.Get(ctx, uri)
}

// TestStages_AdoptingPinnedMemberCompletesAtT1 proves that a pinned member
// that adopted a valid commit completes even when the stop's sweep (t1) runs
// after its CAS to committing: the adopted result is returned, the stage is
// not handed back, and the stop's awaitReported does not return until the
// attempt leaves. The contrast: a sweep before the read returns takes the
// admitted, unstarted member back, and it adopts and starts nothing.
func TestStages_AdoptingPinnedMemberCompletesAtT1(t *testing.T) {
	seed := func(t *testing.T) (*OCIDispatcher, *cacheRuntime, *artifacts.MemoryCommitStore, *orkstackv1.ExecuteTaskInput, *fakeCompleter) {
		t.Helper()
		runtime := &cacheRuntime{}
		d, reports := stoppingDispatcher(t, runtime, artifacts.NewMemoryAttemptStore())
		commits := artifacts.NewMemoryCommitStore()
		d.Commits = commits
		in := baseExportInput("pinned-adopt")
		in.CommitUri = "mem://commits/" + in.GetRunId() + "/pinned-adopt/commit.json"
		seeded, err := d.Execute(context.Background(), in)
		must.OKf(t, err, "seed the commit")
		if seeded.GetCommitAdopted() {
			t.Fatal("the seeding attempt adopted a commit")
		}
		// The retry is a pinned member of the same logical activity.
		pinned := baseExportInput("pinned-adopt")
		pinned.CommitUri = in.GetCommitUri()
		pinned.Pin = &orkstackv1.PinContext{Name: "build"}
		return d, runtime, commits, pinned, reports
	}

	t.Run("the sweep after the CAS", func(t *testing.T) {
		d, runtime, _, in, reports := seed(t)
		_, createsBefore := runtime.counts()
		r := newStopRegistry()
		stage := r.admit()
		defer func() { r.leave(stage) }()

		awaited := make(chan error, 1)
		var swept atomic.Bool
		var owedInHook atomic.Int32
		var handedBackInHook atomic.Bool
		d.Claims = &claimHookStore{AttemptClaimer: d.Claims, after: func() {
			if !swept.CompareAndSwap(false, true) {
				return
			}
			if !stage.committing() {
				t.Error("the first attempt-store write came before the CAS to committing; the test proves nothing")
			}
			r.handBack()
			handedBackInHook.Store(stage.handedBack())
			n, _ := r.owed()
			owedInHook.Store(int32(n))
			go func() { awaited <- r.awaitReported(context.Background()) }()
		}}

		// The Orker is stopping, so it reports the attempt's outcome itself
		// and Execute tells the SDK it has nothing to send.
		returned, err := d.Execute(withStage(context.Background(), stage), in)
		if !errors.Is(err, activity.ErrResultPending) || returned != nil {
			t.Fatalf("Execute = (%v, %v), want (nil, ErrResultPending)", returned, err)
		}
		if !swept.Load() {
			t.Fatal("the sweep never ran; the test proves nothing")
		}
		if len(reports.reports) != 1 || reports.reports[0] != nil {
			t.Fatalf("reports = %v, want one success", reports.reports)
		}
		if result, _ := reports.results[0].(*orkstackv1.ExecuteTaskResult); !result.GetCommitAdopted() {
			t.Errorf("reported result = %v, want the adopted commit", reports.results[0])
		}
		if handedBackInHook.Load() || stage.handedBack() {
			t.Error("the sweep took a committing pinned member back")
		}
		if got := owedInHook.Load(); got != 1 {
			t.Errorf("the stop owes %d attempts while the member reports, want 1", got)
		}
		select {
		case err := <-awaited:
			t.Fatalf("awaitReported returned (%v) before the attempt left", err)
		default:
		}
		r.leave(stage)
		select {
		case err := <-awaited:
			must.OKf(t, err, "awaitReported")
		case <-time.After(10 * time.Second):
			t.Fatal("awaitReported did not return once the attempt left")
		}
		if _, creates := runtime.counts(); creates != createsBefore {
			t.Errorf("an adoption created %d containers", creates-createsBefore)
		}
	})

	t.Run("the sweep before the read returns", func(t *testing.T) {
		d, runtime, commits, in, reports := seed(t)
		pullsBefore, createsBefore := runtime.counts()
		r := newStopRegistry()
		stage := r.admit()
		defer r.leave(stage)
		d.Commits = &getHookCommitStore{CommitStore: commits, before: func() { r.handBack() }}

		result, err := d.Execute(withStage(context.Background(), stage), in)
		if !errors.Is(err, activity.ErrResultPending) || result != nil {
			t.Fatalf("Execute = (%v, %v), want (nil, ErrResultPending): refused unreported, adopting nothing", result, err)
		}
		if !stage.handedBack() {
			t.Error("the sweep did not take the admitted pinned member back")
		}
		if pulls, creates := runtime.counts(); pulls != pullsBefore || creates != createsBefore {
			t.Errorf("a member taken back pulled (%d -> %d) or created (%d -> %d)", pullsBefore, pulls, createsBefore, creates)
		}
		if len(reports.reports) != 0 {
			t.Errorf("a refused pinned member was reported: %v", reports.reports)
		}
	})
}
