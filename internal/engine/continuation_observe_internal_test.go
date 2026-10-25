package engine

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// recordingObserver is a ContinuationObserver that keeps every observation.
type recordingObserver struct {
	mu       sync.Mutex
	writes   []ContinuationWriteObservation
	restores []ContinuationRestoreObservation
}

func (r *recordingObserver) ObserveContinuationWrite(_ context.Context, o ContinuationWriteObservation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writes = append(r.writes, o)
}

func (r *recordingObserver) ObserveContinuationRestore(_ context.Context, o ContinuationRestoreObservation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.restores = append(r.restores, o)
}

func observedRun() *orkstackv1.Run {
	run := restoredRows()
	return run
}

func TestWriteCheckpoint_ObservesSizeOutcomeAndHold(t *testing.T) {
	obs := &recordingObserver{}
	a := NewSnapshotActivities(newMemSnapshotStore()).WithContinuationObserver(obs)
	in := ContinuationObservationInput{Namespace: "team-a", HoldSeconds: 7}
	written, err := a.WriteCheckpoint(context.Background(), observedRun(), 0, 1<<30, in)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.WriteCheckpoint(context.Background(), observedRun(), 0, 8, in); err != nil {
		t.Fatal(err)
	}
	failing := NewSnapshotActivities(&failingStore{err: errors.New("bucket unreachable")}).WithContinuationObserver(obs)
	if _, err := failing.WriteCheckpoint(context.Background(), observedRun(), 0, 1<<30, in); err == nil {
		t.Fatal("a write to a failing store succeeded")
	}
	if len(obs.writes) != 3 {
		t.Fatalf("%d observations, want 3", len(obs.writes))
	}
	if w := obs.writes[0]; w.Outcome != ContinuationOutcomeWritten || w.Kind != "checkpoint" || w.StateBytes != written.GetSizeBytes() ||
		w.HoldSeconds != 7 || w.Namespace != "team-a" {
		t.Fatalf("written = %+v", w)
	}
	if w := obs.writes[1]; w.Outcome != ContinuationOutcomeOverCap || w.StateBytes <= 8 {
		t.Fatalf("over cap = %+v", w)
	}
	if w := obs.writes[2]; w.Outcome != ContinuationOutcomeWriteFailed {
		t.Fatalf("failed = %+v", w)
	}
}

func TestWriteSwitchSnapshot_ObservesItsSize(t *testing.T) {
	obs := &recordingObserver{}
	a := NewSnapshotActivities(newMemSnapshotStore()).WithContinuationObserver(obs)
	obj, err := a.WriteSwitchSnapshot(context.Background(), observedRun(), "team-a", 0, ContinuationObservationInput{Namespace: "team-a"})
	if err != nil {
		t.Fatal(err)
	}
	if len(obs.writes) != 1 || obs.writes[0].Kind != "snapshot" || obs.writes[0].StateBytes != obj.GetSizeBytes() || obs.writes[0].Outcome != "" {
		t.Fatalf("observations = %+v", obs.writes)
	}
}

// readChunkInActivity runs ReadContinuationChunk in a test activity
// environment, failing its first failures attempts.
func readChunkInActivity(t *testing.T, a *SnapshotActivities, uri string, size, offset, length int64, final bool) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivityWithOptions(a.ReadContinuationChunk, activity.RegisterOptions{Name: ActivityReadContinuationChunk})
	if _, err := env.ExecuteActivity(ActivityReadContinuationChunk, ContinuationLocator{URI: uri}, size, offset, length,
		ContinuationObservationInput{Namespace: "team-a", Final: final, RestoreStartedAt: time.Now().Add(-2 * time.Second)}); err != nil {
		t.Fatal(err)
	}
}

func TestReadContinuationChunk_ObservesRestoreOnTheFinalChunk(t *testing.T) {
	store := newMemSnapshotStore()
	a := NewSnapshotActivities(store)
	obj, err := a.WriteCheckpoint(context.Background(), observedRun(), 0, 1<<30, ContinuationObservationInput{Namespace: "team-a"})
	if err != nil {
		t.Fatal(err)
	}
	obs := &recordingObserver{}
	a.WithContinuationObserver(obs)
	size := obj.GetSizeBytes()
	readChunkInActivity(t, a, obj.GetUri(), size, 0, size/2, false)
	readChunkInActivity(t, a, obj.GetUri(), size, size/2, size-size/2, true)
	if len(obs.restores) != 2 {
		t.Fatalf("%d observations, want 2", len(obs.restores))
	}
	if r := obs.restores[0]; r.Final || r.Outcome != "" {
		t.Fatalf("a middle chunk observed %+v", r)
	}
	if r := obs.restores[1]; !r.Final || r.Outcome != ContinuationOutcomeRestored || r.Seconds < 2 {
		t.Fatalf("the final chunk observed %+v, want restored after the restore's own start", r)
	}
}

func TestReadContinuationChunk_ObservesItsAttempt(t *testing.T) {
	store := newMemSnapshotStore()
	a := NewSnapshotActivities(store)
	obj, err := a.WriteCheckpoint(context.Background(), observedRun(), 0, 1<<30, ContinuationObservationInput{Namespace: "team-a"})
	if err != nil {
		t.Fatal(err)
	}
	obs := &recordingObserver{}
	a.WithContinuationObserver(obs)
	readChunkInActivity(t, a, obj.GetUri(), obj.GetSizeBytes(), 0, obj.GetSizeBytes(), true)
	if len(obs.restores) != 1 || obs.restores[0].Attempt != 1 {
		t.Fatalf("observations = %+v, want the first attempt's", obs.restores)
	}
}

func TestContinuationObserver_NilRecordsNothing(t *testing.T) {
	a := NewSnapshotActivities(newMemSnapshotStore())
	if _, err := a.WriteCheckpoint(context.Background(), observedRun(), 0, 1<<30, ContinuationObservationInput{}); err != nil {
		t.Fatalf("a write without an observer failed: %v", err)
	}
	if _, err := a.WriteSwitchSnapshot(context.Background(), observedRun(), "team-a", 0, ContinuationObservationInput{}); err != nil {
		t.Fatalf("a write without an observer failed: %v", err)
	}
}

// TestReadContinuationChunk_SizeMismatchIsCorruptAndNamesNoObject: a
// stored object of another size than the carry names is a non-retryable
// RUN_STATE_CORRUPT, and no error names the object's key.
func TestReadContinuationChunk_SizeMismatchIsCorruptAndNamesNoObject(t *testing.T) {
	store := newMemSnapshotStore()
	a := NewSnapshotActivities(store)
	obj, err := a.WriteCheckpoint(context.Background(), observedRun(), 0, 1<<30, ContinuationObservationInput{})
	if err != nil {
		t.Fatal(err)
	}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivityWithOptions(a.ReadContinuationChunk, activity.RegisterOptions{Name: ActivityReadContinuationChunk})
	_, err = env.ExecuteActivity(ActivityReadContinuationChunk, ContinuationLocator{URI: obj.GetUri()}, obj.GetSizeBytes()+1,
		int64(0), int64(1), ContinuationObservationInput{})
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || app.Type() != RunStateCorruptCode || !app.NonRetryable() {
		t.Fatalf("error = %v, want a non-retryable %s", err, RunStateCorruptCode)
	}
	if name := obj.GetUri()[len(obj.GetUri())-20:]; strings.Contains(err.Error(), name) || strings.Contains(err.Error(), "checkpoint") {
		t.Fatalf("the error names the object: %v", err)
	}
}

// failingStore fails every continuation write.
type failingStore struct {
	memSnapshotStore
	err error
}

func (f *failingStore) WriteContinuation(context.Context, string, string, int64, string, []byte) (string, error) {
	return "", f.err
}

// A read whose key does not resolve fails RUN_STATE_CORRUPT and says what the
// run could not read in its own words: its definition, when the locator names
// a definition passed by reference, and its saved state otherwise.
func TestReadContinuationChunk_AnUnresolvableKeyNamesWhatItWas(t *testing.T) {
	for kind, want := range map[string]string{
		artifacts.ContinuationKindDefinition: "the run's definition is not named by a valid key",
		artifacts.ContinuationKindCheckpoint: "the run's saved state is not named by a valid key",
	} {
		t.Run(kind, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestActivityEnvironment()
			a := NewSnapshotActivities(newMemSnapshotStore())
			env.RegisterActivityWithOptions(a.ReadContinuationChunk, activity.RegisterOptions{Name: ActivityReadContinuationChunk})
			loc := ContinuationLocator{Namespace: "team-a", RunID: "ork-1-2", Generation: 1, Kind: kind, SHA256: "not-a-digest"}
			_, err := env.ExecuteActivity(ActivityReadContinuationChunk, loc, int64(10), int64(0), int64(10),
				ContinuationObservationInput{Namespace: "team-a"})
			var app *temporal.ApplicationError
			if !errors.As(err, &app) || app.Type() != RunStateCorruptCode || !app.NonRetryable() || !strings.Contains(err.Error(), want) {
				t.Fatalf("err = %v, want a non-retryable %s naming %q", err, RunStateCorruptCode, want)
			}
		})
	}
}
