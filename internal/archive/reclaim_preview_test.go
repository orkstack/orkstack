package archive

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPreviewArtifactGC_MatchesPassAndWritesNothing(t *testing.T) {
	w, ids := retiredProducer(t)
	r := w.reclaimer(ReclaimDelete)
	w.warm(r)
	before := w.writes
	got, err := r.Preview(context.Background(), PreviewRequest{Namespace: "default", Samples: 10})
	if err != nil {
		t.Fatal(err)
	}
	if w.writes != before || !w.exists(ids[0]) {
		t.Fatalf("preview wrote (writes %d -> %d)", before, w.writes)
	}
	if tally(ReclaimReport{Objects: got.Objects}, OutcomeWouldDelete, ReasonUnreferenced) != 2 || got.RunsExamined != 1 {
		t.Fatalf("preview = %+v", got)
	}
	if len(got.Samples) != 2 || got.Samples[0].Outcome != OutcomeWouldDelete {
		t.Fatalf("samples = %+v", got.Samples)
	}
	// The pass that follows does what the preview said.
	report := r.RunOnce(context.Background())
	if tally(report, OutcomeDeleted, ReasonUnreferenced) != 2 {
		t.Fatalf("pass deleted %v", report.Objects)
	}
}

func TestPreviewArtifactGC_SamplesNeedANamespace(t *testing.T) {
	w, _ := retiredProducer(t)
	r := w.reclaimer(ReclaimDryRun)
	got, err := r.Preview(context.Background(), PreviewRequest{Samples: 10})
	if err != nil || len(got.Samples) != 0 {
		t.Fatalf("samples without a namespace: %+v, %v", got.Samples, err)
	}
}

func TestPreviewArtifactGC_BoundsAndConcurrency(t *testing.T) {
	w := newWorld(t)
	for _, run := range []string{"ork-p1", "ork-p2", "ork-p3"} {
		w.produce(run, w.now.Add(-72*time.Hour), w.now.Add(-48*time.Hour), "a")
		w.sidecars[run] = w.now.Add(-2 * time.Hour)
	}
	r := w.reclaimer(ReclaimDryRun)
	first, err := r.Preview(context.Background(), PreviewRequest{Limit: 2})
	if err != nil || first.RunsExamined != 2 || first.NextCursor == "" {
		t.Fatalf("first page = %+v, %v", first, err)
	}
	second, err := r.Preview(context.Background(), PreviewRequest{Limit: 2, Cursor: first.NextCursor})
	if err != nil || second.RunsExamined != 1 || second.NextCursor != "" {
		t.Fatalf("second page = %+v, %v", second, err)
	}
	r.previewing.Lock()
	_, err = r.Preview(context.Background(), PreviewRequest{})
	r.previewing.Unlock()
	if !errors.Is(err, ErrPreviewBusy) {
		t.Fatalf("a second preview at once: %v", err)
	}
}

func TestPreviewArtifactGC_ReportsLowWaterRun(t *testing.T) {
	w, _ := retiredProducer(t)
	w.live["ork-old"] = w.now.Add(-50 * time.Hour)
	r := w.reclaimer(ReclaimDryRun)
	// No pass yet: the preview computes a mark of its own, used by nothing.
	got, err := r.Preview(context.Background(), PreviewRequest{Namespace: "default", Samples: 5})
	if err != nil || !got.LowWater.Known || got.LowWater.RunID != "ork-old" || got.WaitingRuns != 1 {
		t.Fatalf("preview = %+v, %v", got, err)
	}
	if got.Samples[0].Reason != ReasonLowWater || got.Samples[0].WaitsFor.IsZero() {
		t.Fatalf("samples = %+v", got.Samples)
	}
	if _, ok := r.LastReport(); ok {
		t.Fatal("a preview recorded a pass")
	}
	// Its mark is not the next pass's previous mark: the next pass warms.
	if report := r.RunOnce(context.Background()); report.Outcome != "warming" {
		t.Fatalf("pass after a preview = %s", report.Outcome)
	}
}

func TestPreviewArtifactGC_ExtrapolatesFromWhenTheStoreClockWasRead(t *testing.T) {
	w, _ := retiredProducer(t)
	w.probeDelay = 30 * time.Second // a slow store: the probe returns 30s after the pass started
	r := w.reclaimer(ReclaimDelete)
	w.warm(r)
	// Retired 15s short of the grace, on the store's clock now. A preview
	// that counted the probe's time twice would read it 15s past.
	w.sidecars["ork-p"] = w.now.Add(-time.Hour + 15*time.Second)
	got, err := r.Preview(context.Background(), PreviewRequest{Namespace: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if n := tally(ReclaimReport{Objects: got.Objects}, OutcomeWouldDelete, ReasonUnreferenced); n != 0 {
		t.Fatalf("a run inside its grace was previewed as due: would delete %d (%+v)", n, got.Objects)
	}
}
