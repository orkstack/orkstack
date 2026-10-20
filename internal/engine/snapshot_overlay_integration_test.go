//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// Stage 6's integration evidence (E6): snapshot overlays on a real cluster
// with their objects in MinIO.

// The snapshot-overlay fixture (testdata/history/README.md), and its
// companion objects (snapshotOverlayObjectsDir, replay_snapshot_overlay_test.go).
const snapshotOverlayFixture = "testdata/history/run-snapshot-overlay-v1.json"

// snapshotOverlayFixtureDocument is the fixture's definition: a chain of
// plain tasks around a fan-out of one item at a time, 44 declared rows. It
// is sequential on purpose: every event lands at the same sequence when the
// payload half re-executes it, so each recorded object can be compared with
// the run current code hands that write.
const snapshotOverlayFixtureDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: overlaid
version: "1.0.0"
inputs:
  regions: {type: array}
tasks:
  - id: prep
    uses: shell
    with: {op: exec}
  - id: convert
    uses: shell
    needs: [prep]
    each: {over: "{{ .inputs.regions }}", max_items: 40, max_parallel: 1}
    with: {op: exec, r: "{{ .each.item }}"}
  - id: a
    uses: shell
    needs: [convert]
    with: {op: exec}
  - id: merge
    uses: shell
    needs: [a]
    with: {op: exec}
`

// snapshotOverlayFixturePolicy is the fixture plan's policy: a floor under
// its 44 rows and a short chain, so a small run records overlays from both
// write sites and several full snapshots between them.
var snapshotOverlayFixturePolicy = engine.OverlayPolicy{MinRows: 16, MaxChain: 6, MaxRows: 256}

// regionsInput is {"regions": [r0 … r(n-1)]}.
func regionsInput(t *testing.T, n int) *orkstackv1.Payload {
	t.Helper()
	regions := make([]any, n)
	for i := range regions {
		regions[i] = fmt.Sprintf("r%d", i)
	}
	data, err := structpb.NewStruct(map[string]any{"regions": regions})
	if err != nil {
		t.Fatal(err)
	}
	return &orkstackv1.Payload{Data: data}
}

// publishedURIs is, for every PublishEvent a history scheduled, its event
// and the snapshot URI it carried.
type publishedURI struct {
	event *orkstackv1.Event
	uri   string
}

func publishedURIs(t *testing.T, h *historypb.History) []publishedURI {
	t.Helper()
	var out []publishedURI
	for _, event := range h.GetEvents() {
		attrs := event.GetActivityTaskScheduledEventAttributes()
		if attrs == nil || attrs.GetActivityType().GetName() != engine.ActivityPublishEvent {
			continue
		}
		var (
			published *orkstackv1.Event
			uri, key  string
		)
		if err := converter.GetDefaultDataConverter().FromPayloads(attrs.GetInput(), &published, &uri, &key); err != nil {
			t.Fatalf("decode a recorded publish: %v", err)
		}
		out = append(out, publishedURI{published, uri})
	}
	return out
}

// TestIntegration_RecordSnapshotOverlayHistory records Stage 6's replay
// fixture from a REAL run on a real cluster, by a versioned worker, with its
// snapshots in MinIO: plain tasks and a fan-out whose plan carries the
// overlay policy, so the history holds the lazy snapshot-overlay marker and
// WriteSnapshotOrOverlay markers from both write sites — emit's and the
// fan-out's fresh item snapshots, an overlay among them — and every full
// snapshot and overlay it wrote, read back byte for byte
// (run-snapshot-overlay-v1.json and its .objects/). It never skips: without
// ORK_RECORD_GOLDEN_HISTORY the recordings land under ORK_PROOF_DIR and the
// committed fixture is left alone. It FAILS unless the recording carries
// the gate, an item-fresh overlay, an emit overlay and at least two full
// snapshots, every overlay reads back as its run, and the history replays.
func TestIntegration_RecordSnapshotOverlayHistory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	s := startCANStack(t, ctx, "overlay-rec", true, canFixtureBehavior)
	write := os.Getenv("ORK_RECORD_GOLDEN_HISTORY") != ""

	runID := fmt.Sprintf("ork-snapshot-overlay-%d", time.Now().UnixNano())
	wf := compile(t, snapshotOverlayFixtureDocument)
	wf.Namespace = "default"
	plan := s.plan(engine.DefaultHistoryMaxEvents, nil)
	policy := snapshotOverlayFixturePolicy
	plan.SnapshotOverlay = &policy
	if err := s.starter.StartRun(ctx, runID, wf, regionsInput(t, 24), plan); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if err := s.client.GetWorkflow(ctx, runID, "").Get(ctx, nil); err != nil {
		t.Fatalf("the run failed: %v", err)
	}
	recorded := s.history(runID, "")
	if versionMarkersOf(t, recorded)[engine.SnapshotOverlayChange] == 0 {
		t.Fatal("the recording carries no snapshot-overlay marker")
	}
	var itemOverlays, emitOverlays int
	objects := map[string][]byte{}
	read := func(uri string) []byte {
		t.Helper()
		data, err := s.snapshots.ReadSnapshot(ctx, uri)
		if err != nil {
			t.Fatalf("read %s back from MinIO: %v", uri, err)
		}
		objects[path.Base(uri)] = data
		return data
	}
	for _, p := range publishedURIs(t, recorded) {
		if p.uri == "" {
			continue
		}
		read(p.uri)
		if !artifacts.IsSnapshotOverlayURI(p.uri) {
			continue
		}
		if p.event.Item != nil {
			itemOverlays++
		} else {
			emitOverlays++
		}
		run, err := engine.MaterializeSnapshotForTest(ctx, s.snapshots, p.uri)
		if err != nil || run.GetId() != runID {
			t.Fatalf("overlay %s does not read back: %v", p.uri, err)
		}
	}
	// Every base an overlay names is among the objects: it is a published
	// event's full snapshot.
	fulls := 0
	for name := range objects {
		if strings.HasSuffix(name, ".pb") {
			fulls++
		}
	}
	if itemOverlays == 0 || emitOverlays == 0 || fulls < 2 {
		t.Fatalf("%d item-fresh overlays, %d emit overlays, %d full snapshots: want at least 1, 1 and 2",
			itemOverlays, emitOverlays, fulls)
	}
	if err := replayRecorded(recorded, runID); err != nil {
		t.Fatalf("the recording does not replay against current code: %v", err)
	}
	files := map[string][]byte{snapshotOverlayFixture: encodeHistory(t, recorded)}
	for name, data := range objects {
		files[filepath.Join(snapshotOverlayObjectsDir, name)] = data
	}
	if write {
		// The objects directory holds exactly this recording's objects.
		if err := os.RemoveAll(snapshotOverlayObjectsDir); err != nil {
			t.Fatal(err)
		}
	}
	for p, data := range files {
		if strings.HasSuffix(p, ".json") {
			proof.Write(t, "snapshot-overlay", filepath.Base(p), data)
		} else if dir := proof.Dir(); dir != "" {
			// Binary objects are written, never logged.
			target := filepath.Join(dir, "snapshot-overlay", "objects", filepath.Base(p))
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(target, data, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		if write {
			if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(p, data, 0o644); err != nil {
				t.Fatalf("write %s: %v", p, err)
			}
		}
	}
	written := "the proof directory only (ORK_RECORD_GOLDEN_HISTORY unset)"
	if write {
		written = "testdata/history"
	}
	proof.WriteJSON(t, "snapshot-overlay", "recorded-fixture.json", map[string]any{
		"run_id": runID, "events": len(recorded.GetEvents()), "objects": len(objects), "full_snapshots": fulls,
		"item_fresh_overlays": itemOverlays, "emit_overlays": emitOverlays,
	})
	t.Logf("recorded %d events of run %s (%d objects: %d full, %d item-fresh and %d emit overlays) into %s, replayed clean",
		len(recorded.GetEvents()), runID, len(objects), fulls, itemOverlays, emitOverlays, written)
}

// countingStore is a snapshot store that counts the bytes and objects of
// every event snapshot written through it: full {seq}.pb and {seq}.ovl.
type countingStore struct {
	engine.SnapshotStore
	fullBytes, overlayBytes atomic.Int64
	fulls, overlays         atomic.Int64
}

func (c *countingStore) WriteSnapshot(ctx context.Context, namespace, runID string, sequence int64, data []byte) (string, error) {
	c.fullBytes.Add(int64(len(data)))
	c.fulls.Add(1)
	return c.SnapshotStore.WriteSnapshot(ctx, namespace, runID, sequence, data)
}

func (c *countingStore) WriteSnapshotOverlay(ctx context.Context, namespace, runID string, sequence int64, data []byte) (string, error) {
	c.overlayBytes.Add(int64(len(data)))
	c.overlays.Add(1)
	return c.SnapshotStore.WriteSnapshotOverlay(ctx, namespace, runID, sequence, data)
}

func (c *countingStore) total() int64 { return c.fullBytes.Load() + c.overlayBytes.Load() }

// overlayFlatDocument is n plain tasks that start together.
func overlayFlatDocument(name string, n int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "apiVersion: orkstack/v1\nkind: Orkflow\nname: %s\nversion: \"1.0.0\"\ntasks:\n", name)
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "  - id: t%04d\n    uses: shell\n    needs: []\n    with: {op: exec}\n", i)
	}
	return b.String()
}

// TestIntegration_SnapshotOverlayBytesForAThousandTasks is E6's figure: the
// same run of 1,000 plain tasks that start together, at the deployment
// defaults (continuation on, its default budget and in-flight cap), once with
// full snapshots — the pre-Stage-6 baseline — and once with the default
// overlay policy. Each run's event-snapshot bytes are counted at the store
// as they are written. The overlay run must write under 100 MB, and must read
// as one run (GetRun, Events, the stream and the archive agree).
func TestIntegration_SnapshotOverlayBytesForAThousandTasks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	defer cancel()
	type outcome struct {
		Bytes, FullBytes, OverlayBytes, Fulls, Overlays int64
		Generations                                     int
		Seconds                                         float64
	}
	measure := func(t *testing.T, label string, policy *engine.OverlayPolicy) outcome {
		s := newChaosStack(t, ctx, "E6/"+label)
		counting := &countingStore{SnapshotStore: s.store}
		s.startReplica("engine", replicaOptions{store: counting})
		s.startProjection(nil)
		s.startOrker("orker", instant)
		budget := engine.DefaultContinuationBudget(engine.DefaultContinuationEvents, engine.DefaultContinuationBytes,
			engine.DefaultContinuationRestoreTimeout)
		plan := s.plan(engine.DefaultHistoryMaxEvents, engine.DefaultHistoryMaxBytes, &budget)
		plan.SnapshotOverlay = policy
		runID := s.newRunID()
		started := time.Now()
		s.start(runID, compile(t, overlayFlatDocument("e6-"+label, 1000)), nil, plan)
		stream := s.openStream(s.reader, runID, 0)
		if status, err := s.wait(runID, 30*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED {
			t.Fatalf("the %s run ended %v: %v", label, status, err)
		}
		elapsed := time.Since(started)
		s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_SUCCEEDED, "")
		return outcome{Bytes: counting.total(), FullBytes: counting.fullBytes.Load(), OverlayBytes: counting.overlayBytes.Load(),
			Fulls: counting.fulls.Load(), Overlays: counting.overlays.Load(), Generations: len(s.chain(runID)),
			Seconds: elapsed.Seconds()}
	}
	policy := engine.DefaultOverlayPolicy()
	var baseline, overlaid outcome
	t.Run("baseline", func(t *testing.T) { baseline = measure(t, "baseline", nil) })
	t.Run("overlays", func(t *testing.T) { overlaid = measure(t, "overlays", &policy) })
	if t.Failed() {
		return
	}
	proof.WriteJSON(t, "snapshot-overlay", "bytes-1000-tasks.json", map[string]any{
		"tasks": 1000, "baseline": baseline, "overlays": overlaid,
		"ratio": float64(overlaid.Bytes) / float64(max(1, baseline.Bytes)),
	})
	t.Logf("E6: 1,000 tasks wrote %d snapshot bytes (%d full) with full snapshots, %d bytes (%d full, %d overlays; %d B full, %d B overlays) with overlays",
		baseline.Bytes, baseline.Fulls, overlaid.Bytes, overlaid.Fulls, overlaid.Overlays, overlaid.FullBytes, overlaid.OverlayBytes)
	if overlaid.Overlays == 0 || overlaid.Bytes >= 100_000_000 || overlaid.Bytes >= baseline.Bytes {
		t.Fatalf("the overlay run wrote %d bytes in %d overlays (baseline %d): want overlays, under 100 MB and under the baseline",
			overlaid.Bytes, overlaid.Overlays, baseline.Bytes)
	}
}

// overlayBytes is what one overlay-bytes run wrote.
type overlayBytes struct {
	Bytes, FullBytes, OverlayBytes, Fulls, Overlays int64
	Generations                                     int
	Seconds                                         float64
}

// measureOverlayBytes runs tasks plain tasks that start together, with
// policy (nil means full snapshots), and counts the event-snapshot bytes the
// store writes.
func measureOverlayBytes(t *testing.T, ctx context.Context, label string, tasks int, policy *engine.OverlayPolicy) overlayBytes {
	t.Helper()
	s := newChaosStack(t, ctx, "E6-reduced/"+label)
	counting := &countingStore{SnapshotStore: s.store}
	s.startReplica("engine", replicaOptions{store: counting})
	s.startProjection(nil)
	s.startOrker("orker", instant)
	budget := engine.DefaultContinuationBudget(engine.DefaultContinuationEvents, engine.DefaultContinuationBytes,
		engine.DefaultContinuationRestoreTimeout)
	plan := s.plan(engine.DefaultHistoryMaxEvents, engine.DefaultHistoryMaxBytes, &budget)
	plan.SnapshotOverlay = policy
	runID := s.newRunID()
	started := time.Now()
	s.start(runID, compile(t, overlayFlatDocument("e6-reduced-"+label, tasks)), nil, plan)
	stream := s.openStream(s.reader, runID, 0)
	if status, err := s.wait(runID, 20*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED {
		t.Fatalf("the %s run ended %v: %v", label, status, err)
	}
	elapsed := time.Since(started)
	s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_SUCCEEDED, "")
	return overlayBytes{Bytes: counting.total(), FullBytes: counting.fullBytes.Load(), OverlayBytes: counting.overlayBytes.Load(),
		Fulls: counting.fulls.Load(), Overlays: counting.overlays.Load(), Generations: len(s.chain(runID)),
		Seconds: elapsed.Seconds()}
}

// TestIntegration_SnapshotOverlaysWriteLessThanFullSnapshots is the merge
// gate's overlay-bytes check: at 400 tasks, above the 256 rows from which
// overlays engage, the same flat stage writes overlays and under half the
// bytes of the same run with full snapshots, and the overlay run reads as
// one run. The absolute bound at 1,000 tasks is the nightly tier's
// (scripts/proof-nightly-tests.txt).
func TestIntegration_SnapshotOverlaysWriteLessThanFullSnapshots(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	const tasks = 400
	policy := engine.DefaultOverlayPolicy()
	var baseline, overlaid overlayBytes
	t.Run("baseline", func(t *testing.T) { baseline = measureOverlayBytes(t, ctx, "baseline", tasks, nil) })
	t.Run("overlays", func(t *testing.T) { overlaid = measureOverlayBytes(t, ctx, "overlays", tasks, &policy) })
	if t.Failed() {
		return
	}
	ratio := float64(overlaid.Bytes) / float64(max(1, baseline.Bytes))
	proof.WriteJSON(t, "snapshot-overlay", fmt.Sprintf("bytes-%d-tasks.json", tasks), map[string]any{
		"tasks": tasks, "baseline": baseline, "overlays": overlaid, "ratio": ratio,
	})
	t.Logf("reduced overlay bytes: %d tasks wrote %d bytes with full snapshots and %d bytes in %d overlays (ratio %.3f)",
		tasks, baseline.Bytes, overlaid.Bytes, overlaid.Overlays, ratio)
	if overlaid.Overlays == 0 || ratio >= 0.5 {
		t.Fatalf("the overlay run wrote %d bytes in %d overlays against the baseline's %d (ratio %.3f): want overlays and under half",
			overlaid.Bytes, overlaid.Overlays, baseline.Bytes, ratio)
	}
}
