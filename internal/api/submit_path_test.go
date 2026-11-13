package api_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/archive"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// The submit path's repair (docs/API.md, "Idempotency keys"): one key, one
// submission, one run id, at most one run; an outcome nobody heard is never
// called a failure; and only two definite "no"s start an id.

// keyedHarness is one engine replica over a stand-in cluster (its starter),
// a claimer and an archive, each of which a test can break.
type keyedHarness struct {
	svc     *api.Service
	runs    *engine.RunStore
	bus     *engine.EventBus
	starter *scriptedStarter
	claimer *sharedClaimer
	archive *breakableArchive
	legacy  int
}

// scriptedStarter is a cluster that admits one start per id, whose next
// starts can be scripted (refused, lost before landing, lost after landing),
// and which is also the cluster's first-start read.
type scriptedStarter struct {
	fakeStarter
	mu      sync.Mutex
	cluster fakeCluster
	script  []startScript
	readErr error
	// foreign holds ids something else started under; with alwaysForeign
	// every id this starter is asked to start is one.
	foreign       map[string]bool
	alwaysForeign bool
}

type startScript struct {
	land bool  // the start lands in the cluster
	err  error // what the caller is answered
}

func (s *scriptedStarter) StartRun(
	ctx context.Context,
	runID string,
	wf *orkstackv1.Orkflow,
	input *orkstackv1.Payload,
	plan engine.RunPlan,
) error {
	s.mu.Lock()
	var step *startScript
	if len(s.script) > 0 {
		step = &s.script[0]
		s.script = s.script[1:]
	}
	if s.alwaysForeign {
		s.foreign[runID] = true
	}
	foreign := s.foreign[runID]
	s.mu.Unlock()
	if foreign {
		return errors.Join(engine.ErrRunIDTaken, serviceerror.NewWorkflowExecutionAlreadyStarted("held", "", ""))
	}
	if step != nil && !step.land {
		return step.err
	}
	if err := s.cluster.start(runID, plan.IdempotencyFingerprint); err != nil {
		return err
	}
	s.fakeStarter.mu.Lock()
	s.fakeStarter.started = append(s.fakeStarter.started, runID)
	s.fakeStarter.plans = append(s.fakeStarter.plans, plan)
	s.fakeStarter.mu.Unlock()
	if step != nil {
		return step.err
	}
	return nil
}

func (s *scriptedStarter) FirstStart(ctx context.Context, runID string) (engine.FirstStartRead, error) {
	s.mu.Lock()
	readErr, foreign := s.readErr, s.foreign[runID]
	s.mu.Unlock()
	if readErr != nil {
		return engine.FirstStartRead{}, readErr
	}
	if foreign {
		return engine.FirstStartRead{Held: true, Fingerprint: "someone-elses"}, nil
	}
	return s.cluster.FirstStart(ctx, runID)
}

func (s *scriptedStarter) next(steps ...startScript) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.script = append(s.script, steps...)
}

func (s *scriptedStarter) markForeign(runID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.foreign[runID] = true
}

// breakableArchive is the archive with injectable read and write failures.
type breakableArchive struct {
	*engine.RunArchiver
	mu       sync.Mutex
	loadErr  error
	writeErr error
}

func (a *breakableArchive) LoadRun(ctx context.Context, runID string) (*orkstackv1.Run, error) {
	a.mu.Lock()
	err := a.loadErr
	a.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return a.RunArchiver.LoadRun(ctx, runID)
}

func (a *breakableArchive) Archive(ctx context.Context, run *orkstackv1.Run, events []*orkstackv1.Event, gaps ...engine.EventGap) error {
	a.mu.Lock()
	err := a.writeErr
	a.mu.Unlock()
	if err != nil {
		return err
	}
	return a.RunArchiver.Archive(ctx, run, events, gaps...)
}

func newKeyedHarness(t *testing.T, mutate ...func(*api.Config)) *keyedHarness {
	t.Helper()
	h := &keyedHarness{
		runs:    engine.NewRunStore(),
		bus:     engine.NewEventBus(),
		starter: &scriptedStarter{foreign: map[string]bool{}},
		claimer: newSharedClaimer(),
		archive: &breakableArchive{RunArchiver: newArchiver(t)},
	}
	cfg := api.Config{
		Orkflows:        engine.NewMemoryOrkflowStore(),
		Runs:            h.runs,
		Bus:             h.bus,
		Starter:         h.starter,
		Archive:         h.archive,
		Idempotency:     h.claimer,
		OnLegacyPending: func(context.Context) { h.legacy++ },
	}
	for _, m := range mutate {
		m(&cfg)
	}
	h.svc = api.NewService(cfg)
	if _, err := h.svc.ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: helloYAML(t)})); err != nil {
		t.Fatalf("ApplyOrkflow: %v", err)
	}
	return h
}

func (h *keyedHarness) submit(key, url string) (*orkstackv1.SubmitRunResponse, error) {
	msg := &orkstackv1.SubmitRunRequest{OrkflowName: "hello-ork", IdempotencyKey: key}
	if url != "" {
		data, err := structpb.NewStruct(map[string]any{"url": url})
		if err != nil {
			return nil, err
		}
		msg.Input = &orkstackv1.Payload{Data: data}
	}
	resp, err := h.svc.SubmitRun(context.Background(), connect.NewRequest(msg))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func (h *keyedHarness) key(k string) string { return claimKeyIn(artifacts.DefaultNamespace, k) }

func (h *keyedHarness) binding(t *testing.T, k string) engine.Claim {
	t.Helper()
	b, bound, err := h.claimer.Lookup(context.Background(), h.key(k))
	must.OKf(t, err, "Lookup")
	if !bound {
		t.Fatalf("key %q is unbound", k)
	}
	return b
}

// strand submits under k with a start that never lands, leaving the key
// naming an id nobody started, and returns that id.
func (h *keyedHarness) strand(t *testing.T, k, url string) string {
	t.Helper()
	h.starter.next(startScript{land: false, err: serviceerror.NewUnavailable("frontend gone")})
	if _, err := h.submit(k, url); connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("stranding submit = %v, want Unavailable", err)
	}
	return h.binding(t, k).RunID
}

func (h *keyedHarness) recordedNothing(t *testing.T, runID string) {
	t.Helper()
	if run, ok := h.runs.Get(runID); ok {
		t.Errorf("run store holds %s (%s), want nothing recorded", runID, run.GetStatus())
	}
	if events := h.bus.Events(runID, 0); len(events) != 0 {
		t.Errorf("%d events published under %s, want none", len(events), runID)
	}
	if _, err := h.archive.RunArchiver.LoadRun(context.Background(), runID); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("archive LoadRun(%s) = %v, want not found", runID, err)
	}
}

// Fault 1: a key whose claimed id never started is not stranded. The retry
// finds the id in neither the cluster nor the archive and starts that same
// id, once.
func TestSubmitKeyed_AClaimedIDThatNeverStartedIsStartedByTheRetry(t *testing.T) {
	h := newKeyedHarness(t)
	claimed := h.strand(t, "order-42", "")
	h.recordedNothing(t, claimed)

	resp, err := h.submit("order-42", "")
	must.OKf(t, err, "retry")
	if resp.GetRun().GetId() != claimed {
		t.Fatalf("retry answered %s, want the key's id %s", resp.GetRun().GetId(), claimed)
	}
	if started := h.starter.startedRuns(); len(started) != 1 || started[0] != claimed {
		t.Fatalf("started %v, want exactly [%s]", started, claimed)
	}
	again, err := h.submit("order-42", "")
	must.OKf(t, err, "third submit")
	if again.GetRun().GetId() != claimed || len(h.starter.startedRuns()) != 1 {
		t.Fatalf("third submit answered %s and started %v", again.GetRun().GetId(), h.starter.startedRuns())
	}
}

// Fault 2: a start whose answer was lost after it landed is not a failure.
// The submission reads the id, finds the start, and records nothing.
func TestSubmitKeyed_ALostStartAnswerThatLandedAnswersTheRun(t *testing.T) {
	for _, key := range []string{"order-42", ""} {
		t.Run("key="+key, func(t *testing.T) {
			h := newKeyedHarness(t)
			h.starter.next(startScript{land: true, err: serviceerror.NewDeadlineExceeded("answer lost")})
			resp, err := h.submit(key, "")
			must.OKf(t, err, "submit")
			id := resp.GetRun().GetId()
			if started := h.starter.startedRuns(); len(started) != 1 || started[0] != id {
				t.Fatalf("started %v, answered %s", started, id)
			}
			if resp.GetRun().GetStatus() == orkstackv1.Status_STATUS_FAILED {
				t.Fatal("a run that landed was answered FAILED")
			}
			if _, err := h.archive.RunArchiver.LoadRun(context.Background(), id); !errors.Is(err, artifacts.ErrNotFound) {
				t.Fatalf("a failed start was archived beside a live run: %v", err)
			}
		})
	}
}

// Fault 2, the whole start table: every unknown answer is unavailable with
// nothing recorded; every refusal is a recorded failed start that the key's
// retry is answered with.
func TestSubmitKeyed_StartAnswersAreClassified(t *testing.T) {
	unknown := map[string]error{
		"deadline":    serviceerror.NewDeadlineExceeded("x"),
		"canceled":    serviceerror.NewCanceled("x"),
		"unavailable": serviceerror.NewUnavailable("x"),
		"internal":    serviceerror.NewInternal("x"),
		"exhausted":   serviceerror.NewResourceExhausted(0, "x"),
		"aborted":     serviceerror.NewAborted("x"),
		"transport":   errors.New("connection reset"),
		"own bound":   context.DeadlineExceeded,
	}
	for name, startErr := range unknown {
		t.Run("unknown/"+name, func(t *testing.T) {
			h := newKeyedHarness(t)
			h.starter.next(startScript{land: false, err: startErr})
			_, err := h.submit("k", "")
			if connect.CodeOf(err) != connect.CodeUnavailable {
				t.Fatalf("submit = %v, want Unavailable", err)
			}
			h.recordedNothing(t, h.binding(t, "k").RunID)
		})
	}
	refused := map[string]error{
		"invalid":      serviceerror.NewInvalidArgument("x"),
		"not found":    serviceerror.NewNotFound("x"),
		"namespace":    serviceerror.NewNamespaceNotFound("x"),
		"denied":       serviceerror.NewPermissionDenied("x", ""),
		"precondition": serviceerror.NewFailedPrecondition("x"),
		"unimpl":       serviceerror.NewUnimplemented("x"),
	}
	for name, startErr := range refused {
		t.Run("refused/"+name, func(t *testing.T) {
			h := newKeyedHarness(t)
			h.starter.next(startScript{land: false, err: startErr})
			if _, err := h.submit("k", ""); err == nil {
				t.Fatal("submit succeeded")
			}
			id := h.binding(t, "k").RunID
			run, err := h.archive.RunArchiver.LoadRun(context.Background(), id)
			must.OKf(t, err, "the refused start is archived")
			if run.GetError().GetCode() != "RUN_START_FAILED" {
				t.Fatalf("archived %v, want RUN_START_FAILED", run.GetError())
			}
			retry, err := h.submit("k", "")
			must.OKf(t, err, "retry")
			if retry.GetRun().GetStatus() != orkstackv1.Status_STATUS_FAILED || len(h.starter.startedRuns()) != 0 {
				t.Fatalf("retry answered %s, started %v; want the recorded failed run",
					retry.GetRun().GetStatus(), h.starter.startedRuns())
			}
		})
	}
}

// Fault 3: a refused start whose record cannot be written is not reported
// as recorded, and the key's retry starts the id rather than naming a run no
// replica can find.
func TestSubmitKeyed_AFailedStartThatCannotBeRecordedSaysSo(t *testing.T) {
	h := newKeyedHarness(t)
	h.archive.writeErr = errors.New("bucket unreachable")
	h.starter.next(startScript{land: false, err: serviceerror.NewFailedPrecondition("refused")})
	_, err := h.submit("k", "")
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("submit = %v, want Unavailable: the record is not durable", err)
	}
	// Nothing was announced that was not recorded: no failed run, no events.
	h.recordedNothing(t, h.binding(t, "k").RunID)
	h.archive.writeErr = nil
	resp, err := h.submit("k", "")
	must.OKf(t, err, "retry")
	if resp.GetRun().GetId() != h.binding(t, "k").RunID || len(h.starter.startedRuns()) != 1 {
		t.Fatalf("retry answered %s and started %v; want the key's id started",
			resp.GetRun().GetId(), h.starter.startedRuns())
	}
}

// Fault 4: a key's id held by something the key did not start moves the
// key once, to a fresh id, and that run starts; nothing is recorded under
// the taken id.
func TestSubmitKeyed_AForeignHolderMovesTheKeyOnce(t *testing.T) {
	h := newKeyedHarness(t)
	stolen := h.strand(t, "k", "")
	h.starter.markForeign(stolen)

	resp, err := h.submit("k", "")
	must.OKf(t, err, "retry")
	moved := h.binding(t, "k").RunID
	if moved == stolen || resp.GetRun().GetId() != moved {
		t.Fatalf("key names %s, answered %s; want a fresh id that started", moved, resp.GetRun().GetId())
	}
	if _, rebinds := h.claimer.counts(); len(rebinds) != 1 {
		t.Fatalf("key moves = %v, want one", rebinds)
	}
	h.recordedNothing(t, stolen)
}

// Fault 4: when the fresh id is taken too, the submission ends unavailable
// and records nothing under either id.
func TestSubmitKeyed_ASecondTakenIDRecordsNothing(t *testing.T) {
	h := newKeyedHarness(t)
	stolen := h.strand(t, "k", "")
	h.starter.markForeign(stolen)
	h.starter.mu.Lock()
	h.starter.alwaysForeign = true
	h.starter.mu.Unlock()

	_, err := h.submit("k", "")
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("submit = %v, want Unavailable", err)
	}
	if started := h.starter.startedRuns(); len(started) != 0 {
		t.Fatalf("started %v", started)
	}
	h.recordedNothing(t, stolen)
	h.recordedNothing(t, h.binding(t, "k").RunID)
}

// Fault 5: a key reused for another submission is refused, and nothing is
// started or moved; the same submission is answered with its run.
func TestSubmitKeyed_AKeyIsBoundToItsSubmission(t *testing.T) {
	h := newKeyedHarness(t)
	first, err := h.submit("k", "https://a.example")
	must.OKf(t, err, "first")
	_, err = h.submit("k", "https://b.example")
	if connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatalf("another input under the key = %v, want AlreadyExists", err)
	}
	same, err := h.submit("k", "https://a.example")
	must.OKf(t, err, "same submission")
	if same.GetRun().GetId() != first.GetRun().GetId() || len(h.starter.startedRuns()) != 1 {
		t.Fatalf("same submission answered %s, started %v", same.GetRun().GetId(), h.starter.startedRuns())
	}
}

// Fault 5 when the lookup gives no answer: the claim's own answer carries
// the fingerprint, and the refusal is made from it before anything starts.
func TestSubmitKeyed_TheFingerprintIsComparedWhenTheLookupFails(t *testing.T) {
	h := newKeyedHarness(t)
	h.strand(t, "k", "https://a.example")
	h.claimer.lookupErr = errors.New("query timed out")
	_, err := h.submit("k", "https://b.example")
	if connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatalf("submit = %v, want AlreadyExists", err)
	}
	if started := h.starter.startedRuns(); len(started) != 0 {
		t.Fatalf("started %v with another submission's input", started)
	}
	resp, err := h.submit("k", "https://a.example")
	must.OKf(t, err, "the key's own retry")
	h.claimer.lookupErr = nil
	if resp.GetRun().GetId() != h.binding(t, "k").RunID {
		t.Fatalf("answered %s", resp.GetRun().GetId())
	}
}

// A claim whose answer was lost after it landed is asked again with the
// same candidate, which is told itself, and the run starts once.
func TestSubmitKeyed_ALostClaimAnswerIsAskedAgain(t *testing.T) {
	h := newKeyedHarness(t)
	h.claimer.claimFailures, h.claimer.claimFailuresLand = 1, true
	resp, err := h.submit("k", "")
	must.OKf(t, err, "submit")
	if resp.GetRun().GetId() != h.binding(t, "k").RunID || len(h.starter.startedRuns()) != 1 {
		t.Fatalf("answered %s, started %v", resp.GetRun().GetId(), h.starter.startedRuns())
	}
}

// Anything that could not be read is unavailable: never a start, never a
// moved key.
func TestSubmitKeyed_AnUnreadableIDStartsNothing(t *testing.T) {
	cases := map[string]func(*keyedHarness){
		"cluster read": func(h *keyedHarness) { h.starter.readErr = serviceerror.NewDeadlineExceeded("slow") },
		"archive read": func(h *keyedHarness) { h.archive.loadErr = errors.New("bucket unreachable") },
	}
	for name, breakIt := range cases {
		t.Run(name, func(t *testing.T) {
			h := newKeyedHarness(t)
			before := h.strand(t, "k", "")
			breakIt(h)
			_, err := h.submit("k", "")
			if connect.CodeOf(err) != connect.CodeUnavailable {
				t.Fatalf("submit = %v, want Unavailable", err)
			}
			if started := h.starter.startedRuns(); len(started) != 0 {
				t.Fatalf("started %v", started)
			}
			if after := h.binding(t, "k").RunID; after != before {
				t.Fatalf("key moved %s -> %s", before, after)
			}
		})
	}
}

// A run the cluster let go and the archive holds is answered from the
// archive, and nothing starts.
func TestSubmitKeyed_AnArchivedRunIsAnsweredNotStarted(t *testing.T) {
	h := newKeyedHarness(t)
	first, err := h.submit("k", "")
	must.OKf(t, err, "first")
	id := first.GetRun().GetId()
	done := &orkstackv1.Run{Id: id, Namespace: artifacts.DefaultNamespace, OrkflowName: "hello-ork",
		Status: orkstackv1.Status_STATUS_SUCCEEDED}
	must.OKf(t, h.archive.RunArchiver.Archive(context.Background(), done, nil), "archive")
	h.starter.cluster.mu.Lock()
	delete(h.starter.cluster.held, id) // the cluster let it go
	h.starter.cluster.mu.Unlock()

	resp, err := h.submit("k", "")
	must.OKf(t, err, "retry")
	if resp.GetRun().GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || len(h.starter.startedRuns()) != 1 {
		t.Fatalf("answered %s, started %v", resp.GetRun().GetStatus(), h.starter.startedRuns())
	}
}

// A binding with no fingerprint (v0.3.0's) is answered as v0.3.0 answered
// it, counted when answered pending, and never started.
func TestSubmitKeyed_ALegacyBindingIsAnsweredAsBefore(t *testing.T) {
	h := newKeyedHarness(t)
	h.claimer.bind(h.key("k"), &orkstackv1.Run{Id: "ork-legacy", OrkflowName: "hello-ork",
		Status: orkstackv1.Status_STATUS_PENDING})
	resp, err := h.submit("k", "https://anything.example")
	must.OKf(t, err, "submit")
	if resp.GetRun().GetId() != "ork-legacy" || resp.GetRun().GetStatus() != orkstackv1.Status_STATUS_PENDING {
		t.Fatalf("answered %s %s", resp.GetRun().GetId(), resp.GetRun().GetStatus())
	}
	if len(h.starter.startedRuns()) != 0 || h.legacy != 1 {
		t.Fatalf("started %v, counted %d legacy answers", h.starter.startedRuns(), h.legacy)
	}
}

// An id is started only under a claim younger than the retention the
// cluster reports.
func TestSubmitKeyed_AnOldClaimStartsNothing(t *testing.T) {
	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	admin := &retentionAdmin{retention: time.Hour}
	h := newKeyedHarness(t, func(c *api.Config) {
		c.Retention = archive.NewRetentionController(nil, admin, "ork-main", time.Hour, nil)
		c.Now = func() time.Time { return now }
	})
	h.strand(t, "k", "")

	h.claimer.claimedAt = now.Add(-2 * time.Hour)
	if _, err := h.submit("k", ""); connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("old claim: submit = %v, want Unavailable", err)
	}
	if len(h.starter.startedRuns()) != 0 {
		t.Fatal("an id was started under a claim older than the cluster's retention")
	}
	h.claimer.claimedAt = now.Add(-time.Minute)
	if _, err := h.submit("k", ""); err != nil {
		t.Fatalf("young claim: %v", err)
	}
	if len(h.starter.startedRuns()) != 1 {
		t.Fatal("a young claim's id was not started")
	}
}

// retentionAdmin reports a fixed cluster retention.
type retentionAdmin struct{ retention time.Duration }

func (a *retentionAdmin) NamespaceRetention(context.Context, string) (time.Duration, error) {
	return a.retention, nil
}

func (a *retentionAdmin) SetNamespaceRetention(_ context.Context, _ string, d time.Duration) (time.Duration, error) {
	a.retention = d
	return d, nil
}

// Two submissions under one key that each find its id held by something
// else: the one whose move lost goes on with the id the other moved the key
// to, and one run starts.
func TestSubmitKeyed_ALostMoveFollowsTheMovedKey(t *testing.T) {
	h := newKeyedHarness(t)
	stolen := h.strand(t, "k", "")
	h.starter.markForeign(stolen)
	// Another submission moves the key first, to an id it has not started
	// yet, just before this submission's own move.
	other := &orkstackv1.Run{Id: "ork-moved-by-other", OrkflowName: "hello-ork"}
	h.claimer.moveFirst = &engine.Claim{RunID: other.GetId(), Fingerprint: h.binding(t, "k").Fingerprint}

	resp, err := h.submit("k", "")
	must.OKf(t, err, "submit")
	if resp.GetRun().GetId() != other.GetId() {
		t.Fatalf("answered %s, want the id the other submission moved the key to, %s", resp.GetRun().GetId(), other.GetId())
	}
	if started := h.starter.startedRuns(); len(started) != 1 || started[0] != other.GetId() {
		t.Fatalf("started %v, want only %s", started, other.GetId())
	}
}

// A claim the cluster refuses outright is not asked again for 30 s.
func TestSubmitKeyed_APermanentClaimErrorIsNotRetried(t *testing.T) {
	h := newKeyedHarness(t)
	h.claimer.err = serviceerror.NewInvalidArgument("bad claim id")
	start := time.Now()
	_, err := h.submit("k", "")
	if connect.CodeOf(err) != connect.CodeInternal {
		t.Fatalf("submit = %v, want Internal", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("a permanent claim error took %s to answer", elapsed)
	}
}
