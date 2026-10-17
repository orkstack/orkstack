//go:build integration

package engine_test

import (
	"context"
	"errors"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// Stage 6's chaos scenarios (T6, C37–C39): a run that writes snapshot
// overlays, on real Temporal and real MinIO, with its objects damaged or one
// of its publishes dropped.

// overlayChaosDocument is a task held at a gate beside a 300-item fan-out:
// 303 declared rows, over the default policy's floor.
const overlayChaosDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: overlay-chaos
version: "1.0.0"
inputs:
  regions: {type: array}
tasks:
  - id: first
    uses: shell
    with: {op: exec}
  - id: held
    uses: shell
    needs: [first]
    with: {op: exec}
  - id: fan
    uses: shell
    needs: [first]
    each: {over: "{{ .inputs.regions }}", max_items: 300, max_parallel: 20}
    with: {op: exec, r: "{{ .each.item }}"}
`

// overlayPlanOn is the stack's plan at the deployment defaults (continuation
// on, its default budget) with the default overlay policy (table TC, T6).
func (s *chaosStack) overlayPlanOn() engine.RunPlan {
	budget := engine.DefaultContinuationBudget(engine.DefaultContinuationEvents, engine.DefaultContinuationBytes,
		engine.DefaultContinuationRestoreTimeout)
	plan := s.plan(engine.DefaultHistoryMaxEvents, engine.DefaultHistoryMaxBytes, &budget)
	policy := engine.DefaultOverlayPolicy()
	plan.SnapshotOverlay = &policy
	return plan
}

// lastPublished is the newest event the run's current history published,
// with its snapshot URI (never an item event between snapshots: those carry
// none).
func (s *chaosStack) lastPublished(runID string) publishedURI {
	s.t.Helper()
	execution, _ := s.current(runID)
	var last publishedURI
	for _, p := range publishedURIs(s.t, s.history(runID, execution)) {
		if p.uri != "" && p.event.GetSequence() > last.event.GetSequence() {
			last = p
		}
	}
	return last
}

// waitPublished blocks until the run's current history has published an
// event matching want.
func (s *chaosStack) waitPublished(runID string, within time.Duration, want func(*orkstackv1.Event) bool) {
	s.t.Helper()
	deadline := time.Now().Add(within)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		execution, _ := s.current(runID)
		for _, p := range publishedURIs(s.t, s.history(runID, execution)) {
			if want(p.event) {
				return
			}
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("run %s published no matching event within %s", runID, within)
		}
		<-ticker.C
	}
}

// overlayHeldRun starts overlayChaosDocument with held at its gate, waits for
// the fan-out to finish, and returns the run, the gates and the newest
// snapshot the run published, which must be an overlay.
func overlayHeldRun(t *testing.T, s *chaosStack) (string, *gates, publishedURI) {
	t.Helper()
	g := newGates()
	s.startReplica("engine", replicaOptions{})
	s.startProjection(nil)
	s.startOrker("orker", gated(g, "held"))
	runID := s.newRunID()
	s.start(runID, compile(t, overlayChaosDocument), regionsInput(t, 300), s.overlayPlanOn())
	g.waitEntered(t, "held", 2*time.Minute)
	s.waitPublished(runID, 5*time.Minute, func(e *orkstackv1.Event) bool {
		return e.GetTaskId() == "fan" && e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED
	})
	last := s.lastPublished(runID)
	if !artifacts.IsSnapshotOverlayURI(last.uri) {
		t.Fatalf("the newest snapshot (event %d) is %s, not an overlay", last.event.GetSequence(), last.uri)
	}
	s.transcript.step("run %s held with fan finished; newest snapshot is event %d's %s", runID,
		last.event.GetSequence(), path.Base(last.uri))
	return runID, g, last
}

// requireUnreadableLive: a live GetRun, through the API and the reader,
// fails Unavailable — an overlay that cannot be assembled — never NotFound
// and never a degraded run.
func requireUnreadableLive(t *testing.T, s *chaosStack, a *chaosAPI, runID string) error {
	t.Helper()
	_, err := a.runs.GetRun(s.ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: runID}))
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("live GetRun = %v, want Unavailable", err)
	}
	if _, readErr := s.reader.GetRun(s.ctx, runID); !errors.Is(readErr, engine.ErrSnapshotOverlayInvalid) ||
		errors.Is(readErr, artifacts.ErrNotFound) {
		t.Fatalf("the reader's live GetRun = %v, want ErrSnapshotOverlayInvalid and never NotFound", readErr)
	}
	s.transcript.step("live GetRun: %v", err)
	return err
}

// finishOverlayRun releases held and requires the run to end SUCCEEDED,
// reading as one run, with a full terminal snapshot; then retires it from
// the cluster and requires the archive's GetRun to hold every row.
func finishOverlayRun(t *testing.T, s *chaosStack, a *chaosAPI, runID string, g *gates) {
	t.Helper()
	g.open("held")
	if status, err := s.wait(runID, 5*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED {
		t.Fatalf("the run ended %v: %v", status, err)
	}
	if last := s.lastPublished(runID); last.event.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED ||
		artifacts.IsSnapshotOverlayURI(last.uri) {
		t.Fatalf("the terminal event %v names %s, want a full snapshot", last.event.GetType(), last.uri)
	}
	s.requireOneRunRead(runID, nil, orkstackv1.Status_STATUS_SUCCEEDED, "")
	chain := s.chain(runID)
	if err := s.reader.DeleteWorkflow(s.ctx, s.namespace, runID); err != nil {
		t.Fatal(err)
	}
	for _, id := range chain {
		s.waitExecutionGone(t, runID, id, time.Minute)
	}
	got, err := a.runs.GetRun(s.ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: runID}))
	if err != nil || got.Msg.GetRun().GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("GetRun from the archive = %v %v", got, err)
	}
	rows := got.Msg.GetRun().GetTaskRuns()
	items := 0
	for _, row := range rows {
		items += len(row.GetItems())
	}
	if len(rows) != 3 || items != 300 {
		t.Fatalf("the archived run holds %d rows and %d items, want 3 and 300", len(rows), items)
	}
	s.transcript.step("retired: GetRun from the archive is SUCCEEDED with %d rows and %d items", len(rows), items)
}

// TestIntegration_C37_OverlayBaseLost: while the run is live, the full
// snapshot its newest overlay names is deleted. A live GetRun then fails
// Unavailable, never NotFound and never a degraded run; the run itself is
// unaffected — its writer holds the verified base — and completes; its
// terminal snapshot is full; the archive and the post-retirement GetRun are
// complete.
func TestIntegration_C37_OverlayBaseLost(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C37")
	a := s.serveAPI(api.Config{})
	runID, g, last := overlayHeldRun(t, s)
	data, err := s.store.ReadSnapshot(ctx, last.uri)
	if err != nil {
		t.Fatal(err)
	}
	overlay := &orkstackv1.RunSnapshotOverlay{}
	if err := proto.Unmarshal(data, overlay); err != nil {
		t.Fatal(err)
	}
	if err := s.store.DeleteSnapshot(ctx, overlay.GetBaseUri()); err != nil {
		t.Fatal(err)
	}
	s.transcript.step("deleted the base %s that %s names", path.Base(overlay.GetBaseUri()), path.Base(last.uri))
	liveErr := requireUnreadableLive(t, s, a, runID)
	finishOverlayRun(t, s, a, runID, g)
	s.transcript.figure("live_get_run", liveErr.Error())
	t.Logf("C37: base %s lost under overlay %s: live GetRun %v; run SUCCEEDED; terminal snapshot full; archive complete",
		path.Base(overlay.GetBaseUri()), path.Base(last.uri), connect.CodeOf(liveErr))
}

// TestIntegration_C38_OverlayCorrupt: the newest overlay is overwritten with
// other bytes. Readers return an error, never a degraded answer; the run
// completes; the archive is complete.
func TestIntegration_C38_OverlayCorrupt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C38")
	a := s.serveAPI(api.Config{})
	runID, g, last := overlayHeldRun(t, s)
	if _, err := s.store.WriteSnapshotOverlay(ctx, "default", runID, last.event.GetSequence(),
		[]byte("these bytes are not a snapshot overlay")); err != nil {
		t.Fatal(err)
	}
	s.transcript.step("overwrote %s with other bytes", path.Base(last.uri))
	liveErr := requireUnreadableLive(t, s, a, runID)
	finishOverlayRun(t, s, a, runID, g)
	s.transcript.figure("live_get_run", liveErr.Error())
	t.Logf("C38: overlay %s corrupted: live GetRun %v; run SUCCEEDED; archive complete", path.Base(last.uri), connect.CodeOf(liveErr))
}

// TestIntegration_C39_ADroppedPublish: the projection worker's connection to
// Temporal is cut for each of one publish's three attempts — the publish of
// the first overlay at or after event 100 — and restored between them, so
// every attempt times out unreported. That publish is logged as failed and
// the run continues. Every later overlay still reads, through no cache,
// because each names a full snapshot, never another overlay; GetRun, Events,
// the stream and the final archive are complete.
func TestIntegration_C39_ADroppedPublish(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C39")
	s.startReplica("engine", replicaOptions{})
	s.startOrker("orker", instant)
	proxy := s.temporalProxy()
	c := s.dial("projection", proxy.Addr())
	inner := engine.NewArchivingEventActivities(s.bus, s.runs, s.archive).WithEventSource(s.reader).WithSnapshotStore(s.store)
	var (
		target   atomic.Int64
		mu       sync.Mutex
		attempts []int32
	)
	projection := worker.New(c, engine.EventQueue, worker.Options{DisableRegistrationAliasing: true, DisableWorkflowWorker: true})
	projection.RegisterActivityWithOptions(func(actx context.Context, event *orkstackv1.Event, uri, key string) error {
		seq := event.GetSequence()
		if target.Load() == 0 && seq >= 100 && artifacts.IsSnapshotOverlayURI(uri) {
			target.CompareAndSwap(0, seq)
		}
		if seq != target.Load() {
			return inner.PublishEvent(actx, event, uri, key)
		}
		mu.Lock()
		attempts = append(attempts, activity.GetInfo(actx).Attempt)
		mu.Unlock()
		// The connection drops while this attempt runs: it can neither
		// finish nor report, so it times out on the cluster's side.
		proxy.Cut()
		<-actx.Done()
		proxy.Restore()
		return actx.Err()
	}, activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	if err := projection.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(projection.Stop)

	runID := s.newRunID()
	plan := s.overlayPlanOn()
	s.start(runID, compile(t, overlayFlatDocument("c39-dropped", 300)), nil, plan)
	stream := s.openStream(s.reader, runID, 0)
	failed := s.logs.wait(t, ctx, "publish event failed", runID, 10*time.Minute)
	s.transcript.step("the run logged %q: %v", failed.Msg, failed.Fields["error"])
	if status, err := s.wait(runID, 10*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED {
		t.Fatalf("the run ended %v: %v", status, err)
	}
	mu.Lock()
	tried := append([]int32(nil), attempts...)
	mu.Unlock()
	if len(tried) != 3 || tried[0] != 1 || tried[2] != 3 {
		t.Fatalf("the dropped publish's attempts were %v, want 1, 2 and 3", tried)
	}
	// Every overlay published after the dropped one reads, uncached, and
	// names a full snapshot.
	dropped := target.Load()
	later := 0
	_, histories := s.histories(runID)
	for _, h := range histories {
		for _, p := range publishedURIs(t, h) {
			if p.event.GetSequence() <= dropped || !artifacts.IsSnapshotOverlayURI(p.uri) {
				continue
			}
			data, err := s.store.ReadSnapshot(ctx, p.uri)
			if err != nil {
				t.Fatalf("event %d's overlay: %v", p.event.GetSequence(), err)
			}
			overlay := &orkstackv1.RunSnapshotOverlay{}
			if err := proto.Unmarshal(data, overlay); err != nil {
				t.Fatal(err)
			}
			if !strings.HasSuffix(overlay.GetBaseUri(), ".pb") {
				t.Fatalf("event %d's overlay names %s, not a full snapshot", p.event.GetSequence(), overlay.GetBaseUri())
			}
			if _, err := engine.MaterializeSnapshotForTest(ctx, s.store, p.uri); err != nil {
				t.Fatalf("event %d's overlay does not read: %v", p.event.GetSequence(), err)
			}
			later++
		}
	}
	if later == 0 {
		t.Fatal("no overlay was published after the dropped one")
	}
	events := s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_SUCCEEDED, "")
	s.transcript.figure("dropped_publish", map[string]any{"sequence": dropped, "attempts": tried, "later_overlays_read": later,
		"events": len(events)})
	t.Logf("C39: event %d's publish dropped after attempts %v; run SUCCEEDED; %d later overlays read uncached; "+
		"%d events: GetRun, Events, stream and archive agree", dropped, tried, later, len(events))
}
