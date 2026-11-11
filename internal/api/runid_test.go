package api_test

import (
	"context"
	"errors"
	"regexp"
	"slices"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// runIDShape is the documented id shape, `ork-` and a UUIDv7 in base32.
var runIDShape = regexp.MustCompile(`^ork-[0-7][0-9a-hjkmnp-tv-z]{25}$`)

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

func (s *takenIDStarter) CancelRun(context.Context, string, engine.CancelRequest) error { return nil }
func (s *takenIDStarter) SignalAwait(context.Context, string, *orkstackv1.AwaitSignal) error {
	return nil
}

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

// TestSubmitRun_DefaultRunIDHasTheDocumentedShape pins, at the API surface,
// what the built-in generator issues.
func TestSubmitRun_DefaultRunIDHasTheDocumentedShape(t *testing.T) {
	f := newRunIDFixture(t, &fakeStarter{}, nil)

	run, err := f.submit(t, "")
	must.OKf(t, err, "SubmitRun")
	if id := run.GetId(); !runIDShape.MatchString(id) {
		t.Fatalf("run id = %q, want a match for %s", id, runIDShape)
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
	must.OKf(t, err, "SubmitRun")

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
		t.Errorf("reissued run id = %q, want a match for %s", run.GetId(), runIDShape)
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
	must.OKf(t, err, "ListRuns")
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
	must.OKf(t, err, "SubmitRun")
	second, err := f.submit(t, "order-42")
	must.OKf(t, err, "repeat SubmitRun")
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
	must.OKf(t, err, "GetRun(%q)", last)
	if status := got.Msg.GetRun().GetStatus(); status != orkstackv1.Status_STATUS_FAILED {
		t.Errorf("run %q status = %v, want FAILED", last, status)
	}
}

// TestSubmitRun_GeneratedRunIDSurvivesTheDurablePath pins the id's shape where
// it can actually break: the id is an object key in the archive, so the widened
// sequence has to encode, prefix-scan and decode back verbatim. The second
// Service stands for a replica that never saw the run.
func TestSubmitRun_GeneratedRunIDSurvivesTheDurablePath(t *testing.T) {
	archive := newArchiver(t)
	f := newRunIDFixture(t, &fakeStarter{}, archive)

	run, err := f.submit(t, "")
	must.OKf(t, err, "SubmitRun")
	id := run.GetId()
	if len(id) > 56 {
		// pkg/artifacts/s3_manifest.go s3MaxComponent bounds one key segment
		// at 56 bytes; a longer id would be replaced by a digest and stop
		// being readable in an S3 key.
		t.Errorf("run id %q is %d bytes, longer than one S3 key segment holds verbatim", id, len(id))
	}

	ctx := context.Background()
	run.Status = orkstackv1.Status_STATUS_SUCCEEDED
	must.OKf(t, archive.Archive(ctx, run, f.bus.Events(id, 0)), "Archive")

	replica := api.NewService(api.Config{
		Orkflows: engine.NewMemoryOrkflowStore(),
		Runs:     engine.NewRunStore(),
		Bus:      engine.NewEventBus(),
		Starter:  &fakeStarter{},
		Archive:  archive,
	})
	got, err := replica.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: id}))
	must.OKf(t, err, "GetRun(%q) on a replica that never saw the run", id)
	if got.Msg.GetRun().GetId() != id {
		t.Errorf("archived run id = %q, want %q unchanged", got.Msg.GetRun().GetId(), id)
	}
	listed, err := replica.ListRuns(ctx, connect.NewRequest(&orkstackv1.ListRunsRequest{}))
	must.OKf(t, err, "ListRuns")
	if len(listed.Msg.GetRuns()) != 1 || listed.Msg.GetRuns()[0].GetId() != id {
		t.Errorf("ListRuns = %v, want exactly [%s]", listed.Msg.GetRuns(), id)
	}
}

// TestListRuns_OrdersRunsByTimeNotByRunID pins the id's shape at the one place
// in the codebase where a run id is compared to another: mergeRuns' tie-break
// (internal/api/service.go). Ordering must come from the run's
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
	must.OKf(t, err, "ListRuns")
	got := make([]string, len(listed.Msg.GetRuns()))
	for i, run := range listed.Msg.GetRuns() {
		got[i] = run.GetId()
	}
	want := []string{ordered[0].id, ordered[1].id, ordered[2].id}
	if !slices.Equal(got, want) {
		t.Errorf("ListRuns order = %v, want newest first %v", got, want)
	}
}

// TestRunIDs_OldAndNewShapesCoexist: a deployment's archive and cluster hold runs
// of the older shape beside new ones. Every read treats both as opaque, so a list
// is ordered by time whichever shape each run has, and each is found by its id.
func TestRunIDs_OldAndNewShapesCoexist(t *testing.T) {
	base := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	runs := engine.NewRunStore()
	svc := api.NewService(api.Config{
		Orkflows: engine.NewMemoryOrkflowStore(),
		Runs:     runs,
		Bus:      engine.NewEventBus(),
		Starter:  &fakeStarter{},
		Archive:  newArchiver(t),
	})

	newer := engine.NewRunID()
	if !runIDShape.MatchString(newer) {
		t.Fatalf("NewRunID() = %q, want a match for %s", newer, runIDShape)
	}
	// Newest first, the two shapes interleaved, with the same timestamp on the
	// outer pair so the id's documented tie-break has to compare across shapes.
	ordered := []struct {
		id  string
		age time.Duration
	}{
		{newer, 0},
		{"ork-1786928035776242000-4000000000000002", 0},
		{engine.NewRunID(), time.Minute},
		{"ork-1786928035776242000-1000000000000001", 2 * time.Minute},
	}
	for _, run := range ordered {
		runs.Save(&orkstackv1.Run{
			Id:          run.id,
			OrkflowName: "hello-ork",
			Status:      orkstackv1.Status_STATUS_RUNNING,
			CreatedAt:   timestamppb.New(base.Add(-run.age)),
		})
	}

	listed, err := svc.ListRuns(context.Background(), connect.NewRequest(&orkstackv1.ListRunsRequest{}))
	must.OKf(t, err, "ListRuns")
	if len(listed.Msg.GetRuns()) != len(ordered) {
		t.Fatalf("ListRuns returned %d runs, want %d", len(listed.Msg.GetRuns()), len(ordered))
	}
	for i, run := range listed.Msg.GetRuns()[2:] {
		if want := ordered[i+2].id; run.GetId() != want {
			t.Errorf("ListRuns[%d] = %q, want %q", i+2, run.GetId(), want)
		}
	}
	for _, run := range ordered {
		got, err := svc.GetRun(context.Background(), connect.NewRequest(&orkstackv1.GetRunRequest{RunId: run.id}))
		if err != nil || got.Msg.GetRun().GetId() != run.id {
			t.Errorf("GetRun(%s) = %v, %v", run.id, got, err)
		}
	}
}
