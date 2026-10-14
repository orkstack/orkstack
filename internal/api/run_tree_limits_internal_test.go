package api

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/engine"
)

// TestPinRunTreeLimits_PinsEveryCommandStreamNumber: the top-level plan
// carries every number the guard, the writer, the hold, the bootstrap and
// the in-flight cap decide with — every reserve term and byte charge, every
// budget field, the restore policy and both in-flight numbers — equal to the
// engine's defaults and the deployment's configuration, so a release that
// changes a default moves only new runs; and no child plan nested in it
// carries any of them.
func TestPinRunTreeLimits_PinsEveryCommandStreamNumber(t *testing.T) {
	svc := NewService(Config{
		HistoryMaxEvents: 40000, HistoryMaxBytes: 40 << 20,
		ContinuationEnabled: true, ContinuationEvents: 5000, ContinuationBytes: 4 << 20,
		ContinuationRestoreTimeout: 20 * time.Minute, RunMaxInFlight: 300,
	})
	child := &engine.ChildRunPlan{RunID: "ork-1-1", Plan: engine.RunPlan{Namespace: "default"}}
	plan := engine.RunPlan{Namespace: "default", Children: map[string]*engine.ChildRunPlan{"compose": child}}
	before, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	svc.pinRunTreeLimits(&plan)

	want := engine.HistoryLimit{MaxEvents: 40000, MaxBytes: 40 << 20, Reserve: engine.DefaultHistoryReserve}
	if plan.History == nil || *plan.History != want {
		t.Fatalf("history = %+v, want %+v", plan.History, want)
	}
	budget := plan.Continuation
	if budget == nil {
		t.Fatal("no continuation budget with continuation enabled")
	}
	wantBudget := engine.ContinuationBudget{
		Events: 5000, Bytes: 4 << 20, CheckpointMaxBytes: 4 << 20, RetryEvents: 1250, RetryBytes: 1 << 20,
		HoldPercent:    engine.DefaultContinuationHoldPercent,
		MaxGenerations: engine.DefaultContinuationMaxGenerations,
		ChunkBytes:     engine.DefaultContinuationChunkBytes, ChunksPerTask: engine.DefaultContinuationChunksPerTask,
		ItemInputFloorBytes: engine.DefaultItemInputFloorBytes,
		Restore: engine.RestorePolicy{
			TimeoutSeconds:       1200,
			AttemptSeconds:       int64(engine.DefaultRestoreAttempt / time.Second),
			BackoffInitialMillis: int64(engine.DefaultRestoreBackoffInitial / time.Millisecond),
			BackoffMaxSeconds:    int64(engine.DefaultRestoreBackoffMax / time.Second),
		},
	}
	if *budget != wantBudget {
		t.Fatalf("budget = %+v\nwant     %+v", *budget, wantBudget)
	}
	if plan.InFlight == nil || plan.InFlight.Max != 300 || plan.InFlight.HardStop != engine.DefaultPendingActivityHardStop {
		t.Fatalf("in-flight = %+v", plan.InFlight)
	}
	if nested := plan.Children["compose"].Plan; nested.History != nil || nested.Continuation != nil || nested.InFlight != nil {
		t.Fatal("a child plan nested in the top-level plan carries run-tree limits")
	}
	// Every field is encoded: none is dropped by omitempty at a default.
	encoded, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	// Once per run tree, and under a kilobyte (spec: about 0.8 KB).
	if growth := len(encoded) - len(before); growth >= 1024 {
		t.Fatalf("the run-tree limits grew the plan by %d bytes, want under 1 KiB", growth)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	continuation, _ := decoded["continuation"].(map[string]any)
	for _, key := range []string{"events", "bytes", "checkpoint_max_bytes", "retry_events", "retry_bytes", "hold_percent",
		"max_generations", "chunk_bytes", "chunks_per_task", "restore"} {
		if _, ok := continuation[key]; !ok {
			t.Errorf("the encoded budget has no %q", key)
		}
	}

	// Continuation off: no budget, the cap still on, at its default.
	off := NewService(Config{})
	var plain engine.RunPlan
	off.pinRunTreeLimits(&plain)
	if plain.Continuation != nil {
		t.Fatal("a deployment with continuation off pinned a budget")
	}
	if plain.InFlight == nil || plain.InFlight.Max != engine.DefaultRunMaxInFlight {
		t.Fatalf("in-flight with defaults = %+v, want %d", plain.InFlight, engine.DefaultRunMaxInFlight)
	}
	t.Logf("E2 run-tree limits: the top-level plan grows by %d bytes with continuation on", len(encoded)-len(before))
}
