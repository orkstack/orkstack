package api_test

import (
	"context"
	"errors"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// runIDShape is the documented id shape, ork-{timestamp}-{sequence}.
var runIDShape = regexp.MustCompile(`^ork-\d+-\d+$`)

// takenIDStarter answers the first rejections StartRun calls the way a cluster
// answers a workflow id another replica already owns, and records every id it
// was asked to start.
type takenIDStarter struct {
	mu         sync.Mutex
	rejections int
	rejected   []string
	started    []string
}

func (s *takenIDStarter) StartRun(
	_ context.Context,
	runID string,
	_ *orkstackv1.Orkflow,
	_ *orkstackv1.Payload,
	_ engine.RunPlan,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rejections > 0 {
		s.rejections--
		s.rejected = append(s.rejected, runID)
		return errors.Join(engine.ErrRunIDTaken, serviceerror.NewWorkflowExecutionAlreadyStarted(
			"Workflow execution is already running", "start-request-id", "temporal-run-id"))
	}
	s.started = append(s.started, runID)
	return nil
}

func (s *takenIDStarter) CancelRun(context.Context, string, string) error { return nil }

func (s *takenIDStarter) attempts() (rejected, started []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.rejected...), append([]string(nil), s.started...)
}

// runIDFixture is a Service wired to its own stores with the built-in run id
// generator, which is what production uses.
type runIDFixture struct {
	svc     *api.Service
	runs    *engine.RunStore
	bus     *engine.EventBus
	starter api.RunStarter
}

func newRunIDFixture(t *testing.T, starter api.RunStarter, archive api.RunArchive) *runIDFixture {
	t.Helper()
	runs, bus := engine.NewRunStore(), engine.NewEventBus()
	svc := api.NewService(api.Config{
		Orkflows: engine.NewMemoryOrkflowStore(),
		Runs:     runs,
		Bus:      bus,
		Starter:  starter,
		Archive:  archive,
	})
	if _, err := svc.ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: helloYAML(t)})); err != nil {
		t.Fatalf("ApplyOrkflow: %v", err)
	}
	return &runIDFixture{svc: svc, runs: runs, bus: bus, starter: starter}
}

func (f *runIDFixture) submit(t *testing.T, key string) (*orkstackv1.Run, error) {
	t.Helper()
	resp, err := f.svc.SubmitRun(context.Background(),
		connect.NewRequest(&orkstackv1.SubmitRunRequest{
			OrkflowName:    "hello-ork",
			IdempotencyKey: key,
		}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetRun(), nil
}

// TestSubmitRun_DefaultRunIDDoesNotRestartAtOnePerProcess is acceptance 3 at
// the API surface: before M8.1 the first run of every replica was
// ork-<nanos>-1, so two replicas' logs read as if they were the same series.
func TestSubmitRun_DefaultRunIDDoesNotRestartAtOnePerProcess(t *testing.T) {
	f := newRunIDFixture(t, &fakeStarter{}, nil)

	run, err := f.submit(t, "")
	if err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	id := run.GetId()
	if !runIDShape.MatchString(id) {
		t.Fatalf("run id = %q, want ork-{timestamp}-{sequence}", id)
	}
	sequence := id[strings.LastIndex(id, "-")+1:]
	if sequence == "1" {
		t.Errorf("run id = %q: the sequence still restarts at 1 per process", id)
	}
}

// TestSubmitRun_ConcurrentSubmissionsGetDistinctRunIDs keeps the property the
// per-process counter did provide. The cross-replica half is
// engine.TestNewRunID_UniqueAcrossSeparateProcesses.
func TestSubmitRun_ConcurrentSubmissionsGetDistinctRunIDs(t *testing.T) {
	f := newRunIDFixture(t, &fakeStarter{}, nil)

	const submissions = 200
	ids := make([]string, submissions)
	var wg sync.WaitGroup
	for i := range submissions {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			run, err := f.submit(t, "")
			if err != nil {
				t.Errorf("SubmitRun: %v", err)
				return
			}
			ids[i] = run.GetId()
		}(i)
	}
	wg.Wait()

	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("run id %q was issued twice", id)
		}
		seen[id] = true
	}
}

// TestSubmitRun_ReissuesARunIDAnotherReplicaOwns is the API half of the
// server-decided guarantee: when Temporal refuses the candidate id, the
// submission continues under a fresh one instead of failing or, worse,
// reporting the other replica's run.
func TestSubmitRun_ReissuesARunIDAnotherReplicaOwns(t *testing.T) {
	starter := &takenIDStarter{rejections: 1}
	f := newRunIDFixture(t, starter, nil)

	run, err := f.submit(t, "")
	if err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}

	rejected, started := starter.attempts()
	if len(rejected) != 1 || len(started) != 1 {
		t.Fatalf("StartRun attempts: rejected %v, started %v; want one of each", rejected, started)
	}
	if run.GetId() != started[0] {
		t.Errorf("SubmitRun returned %q, want the id that actually started, %q", run.GetId(), started[0])
	}
	if run.GetId() == rejected[0] {
		t.Errorf("SubmitRun returned the rejected id %q", rejected[0])
	}
	if !runIDShape.MatchString(run.GetId()) {
		t.Errorf("reissued run id = %q, want ork-{timestamp}-{sequence}", run.GetId())
	}

	// The discarded candidate names a run that never existed anywhere. It must
	// not linger in the read model as a run stuck in PENDING.
	_, err = f.svc.GetRun(context.Background(),
		connect.NewRequest(&orkstackv1.GetRunRequest{RunId: rejected[0]}))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("GetRun(%q) code = %v, want NotFound: the discarded candidate is still readable",
			rejected[0], connect.CodeOf(err))
	}
	listed, err := f.svc.ListRuns(context.Background(),
		connect.NewRequest(&orkstackv1.ListRunsRequest{}))
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if got := len(listed.Msg.GetRuns()); got != 1 {
		t.Errorf("ListRuns returned %d runs, want 1", got)
	}
	if events := f.bus.Events(rejected[0], 0); len(events) != 0 {
		t.Errorf("the discarded candidate still has %d events on the bus", len(events))
	}
}

// TestSubmitRun_KeyedSubmissionFollowsTheReissuedRunID keeps idempotency
// pointing at the run that exists: a key bound to the discarded candidate
// would make every repeat submission fail.
func TestSubmitRun_KeyedSubmissionFollowsTheReissuedRunID(t *testing.T) {
	starter := &takenIDStarter{rejections: 1}
	f := newRunIDFixture(t, starter, nil)

	first, err := f.submit(t, "order-42")
	if err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	second, err := f.submit(t, "order-42")
	if err != nil {
		t.Fatalf("repeat SubmitRun: %v", err)
	}
	if second.GetId() != first.GetId() {
		t.Errorf("repeat submission returned %q, want the first run %q", second.GetId(), first.GetId())
	}
	if _, started := starter.attempts(); len(started) != 1 {
		t.Errorf("StartRun ran %d times, want once: the key must not start a second run", len(started))
	}
}

// TestSubmitRun_FailsWhenTheReissuedRunIDIsAlsoTaken bounds the retry. Two
// collisions in a row is not bad luck, it is a broken generator, and a
// submission that keeps drawing ids would hide it.
func TestSubmitRun_FailsWhenTheReissuedRunIDIsAlsoTaken(t *testing.T) {
	starter := &takenIDStarter{rejections: 10}
	f := newRunIDFixture(t, starter, nil)

	if _, err := f.submit(t, ""); err == nil {
		t.Fatal("SubmitRun succeeded, want the repeated collision reported")
	}
	rejected, started := starter.attempts()
	if len(started) != 0 {
		t.Errorf("StartRun started %v, want nothing", started)
	}
	if len(rejected) != 2 {
		t.Errorf("SubmitRun tried %d ids (%v), want exactly two: the candidate and one reissue",
			len(rejected), rejected)
	}
	if len(rejected) == 2 && rejected[0] == rejected[1] {
		t.Errorf("the retry reused the taken id %q", rejected[0])
	}

	// A run that never reached Temporal still ends, exactly as any other
	// failed start does.
	last := rejected[len(rejected)-1]
	got, err := f.svc.GetRun(context.Background(),
		connect.NewRequest(&orkstackv1.GetRunRequest{RunId: last}))
	if err != nil {
		t.Fatalf("GetRun(%q): %v", last, err)
	}
	if status := got.Msg.GetRun().GetStatus(); status != orkstackv1.Status_STATUS_FAILED {
		t.Errorf("run %q status = %v, want FAILED", last, status)
	}
}

// TestSubmitRun_GeneratedRunIDSurvivesTheDurablePath is acceptance 5 where it
// can actually break: the id is an object key in the archive, so the widened
// sequence has to encode, prefix-scan and decode back verbatim. The second
// Service stands for a replica that never saw the run.
func TestSubmitRun_GeneratedRunIDSurvivesTheDurablePath(t *testing.T) {
	archive := newArchiver(t)
	f := newRunIDFixture(t, &fakeStarter{}, archive)

	run, err := f.submit(t, "")
	if err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	id := run.GetId()
	if len(id) > 56 {
		// pkg/artifacts/s3_manifest.go s3MaxComponent bounds one key segment
		// at 56 bytes; a longer id would be replaced by a digest and stop
		// being readable in an S3 key.
		t.Errorf("run id %q is %d bytes, longer than one S3 key segment holds verbatim", id, len(id))
	}

	ctx := context.Background()
	run.Status = orkstackv1.Status_STATUS_SUCCEEDED
	if err := archive.Archive(ctx, run, f.bus.Events(id, 0)); err != nil {
		t.Fatalf("Archive: %v", err)
	}

	replica := api.NewService(api.Config{
		Orkflows: engine.NewMemoryOrkflowStore(),
		Runs:     engine.NewRunStore(),
		Bus:      engine.NewEventBus(),
		Starter:  &fakeStarter{},
		Archive:  archive,
	})
	got, err := replica.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: id}))
	if err != nil {
		t.Fatalf("GetRun(%q) on a replica that never saw the run: %v", id, err)
	}
	if got.Msg.GetRun().GetId() != id {
		t.Errorf("archived run id = %q, want %q unchanged", got.Msg.GetRun().GetId(), id)
	}
	listed, err := replica.ListRuns(ctx, connect.NewRequest(&orkstackv1.ListRunsRequest{}))
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(listed.Msg.GetRuns()) != 1 || listed.Msg.GetRuns()[0].GetId() != id {
		t.Errorf("ListRuns = %v, want exactly [%s]", listed.Msg.GetRuns(), id)
	}
}

// TestListRuns_OrdersRunsByTimeNotByRunID is acceptance 5 at the one place in
// the codebase where a run id is compared to another: mergeRuns' tie-break
// (internal/api/service.go:517-523). Ordering must come from the run's
// timestamps, so a wider, no-longer-ascending sequence cannot reorder a page.
// The two nonces stand for two replicas whose ids interleave in time.
func TestListRuns_OrdersRunsByTimeNotByRunID(t *testing.T) {
	base := time.Date(2026, 8, 17, 0, 53, 55, 0, time.UTC)
	archive := newArchiver(t)
	runs := engine.NewRunStore()
	svc := api.NewService(api.Config{
		Orkflows: engine.NewMemoryOrkflowStore(),
		Runs:     runs,
		Bus:      engine.NewEventBus(),
		Starter:  &fakeStarter{},
		Archive:  archive,
	})

	// Deliberately adversarial: the newest run carries the smallest id and the
	// oldest the largest, so any ordering that leaned on the id would invert.
	ordered := []struct {
		id  string
		age time.Duration
	}{
		{"ork-1786928035776242000-1000000000000001", 0},
		{"ork-1786928035776242000-4000000000000002", time.Minute},
		{"ork-1786928035776242000-9000000000000003", 2 * time.Minute},
	}
	for _, run := range ordered {
		runs.Save(&orkstackv1.Run{
			Id:          run.id,
			OrkflowName: "hello-ork",
			Status:      orkstackv1.Status_STATUS_RUNNING,
			CreatedAt:   timestamppb.New(base.Add(-run.age)),
		})
	}

	listed, err := svc.ListRuns(context.Background(),
		connect.NewRequest(&orkstackv1.ListRunsRequest{}))
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	got := make([]string, len(listed.Msg.GetRuns()))
	for i, run := range listed.Msg.GetRuns() {
		got[i] = run.GetId()
	}
	want := []string{ordered[0].id, ordered[1].id, ordered[2].id}
	if !slices.Equal(got, want) {
		t.Errorf("ListRuns order = %v, want newest first %v", got, want)
	}
}
