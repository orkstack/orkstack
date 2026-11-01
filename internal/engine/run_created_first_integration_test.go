//go:build integration

package engine_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// storeHooks is a replica's snapshot store with onWrite in front of every
// event snapshot write (WriteSnapshot), given the event's sequence and the
// local activity's attempt — a non-nil error fails that attempt, as the
// store would — and onRead, when set, in front of every continuation read.
type storeHooks struct {
	engine.SnapshotStore
	onWrite func(sequence int64, attempt int32) error
	onRead  func()
}

func (h storeHooks) WriteSnapshot(ctx context.Context, namespace, runID string, sequence int64, data []byte) (string, error) {
	if err := h.onWrite(sequence, activity.GetInfo(ctx).Attempt); err != nil {
		return "", err
	}
	return h.SnapshotStore.WriteSnapshot(ctx, namespace, runID, sequence, data)
}

func (h storeHooks) ReadContinuationRange(ctx context.Context, uri string, wantSize, offset, length int64) ([]byte, error) {
	if h.onRead != nil {
		h.onRead()
	}
	return h.SnapshotStore.ReadContinuationRange(ctx, uri, wantSize, offset, length)
}

// runCreatedWrites follows RUN_CREATED's write in a run's ending before
// start: each local activity that writes sequence 1 starts at attempt 1, so
// the n-th attempt-1 write of sequence 1 is that sequence's n-th write.
type runCreatedWrites struct {
	mu     sync.Mutex
	writes int // writes of sequence 1 begun
	tries  int // attempts at sequence 1
	others int // attempts at any other sequence
}

// note records one attempt and returns which write of sequence 1 it belongs
// to (0 for another sequence).
func (w *runCreatedWrites) note(sequence int64, attempt int32) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	if sequence != 1 {
		w.others++
		return 0
	}
	w.tries++
	if attempt == 1 {
		w.writes++
	}
	return w.writes
}

func (w *runCreatedWrites) counts() (writes, tries, others int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writes, w.tries, w.others
}

// runCreatedFirstRecordings is what each run-created-first fixture's run
// meets and publishes: how many of RUN_CREATED's writes the store refuses
// (every attempt of each), the stream, and how many writes of sequence 1
// begin.
var runCreatedFirstRecordings = map[string]struct {
	refused int
	want    string
	writes  int
}{
	"gated":  {1, "[1:RUN_CREATED 2:RUN_FAILED]", 2},
	"gap":    {2, "[2:RUN_FAILED]", 2},
	"legacy": {1, "[2:RUN_FAILED]", 1},
}

// corruptStart starts generation 0 of a run whose definition passed by
// reference names another version than its stub, so it ends
// RUN_DEFINITION_CORRUPT through endBeforeStart, on a replica whose store
// refuses every attempt at an event snapshot that refuse names (the n-th
// write of sequence 1, 0 for another sequence), and waits for it to close
// Failed. It returns the run id, its plan, its definition object's URI and
// the writes it made.
func (s *chaosStack) corruptStart(t *testing.T, refuse func(n int, sequence int64) bool) (string, engine.RunPlan, string, *runCreatedWrites) {
	t.Helper()
	writes := &runCreatedWrites{}
	refused := errors.New("the store refuses the event's snapshot")
	store := storeHooks{SnapshotStore: s.store, onWrite: func(sequence int64, attempt int32) error {
		if refuse(writes.note(sequence, attempt), sequence) {
			return refused
		}
		return nil
	}}
	s.startReplica("replica-a", replicaOptions{store: store})
	s.startProjection(nil)
	s.startOrker("orker-1", instant)
	runID := s.newRunID()
	s.transcript.run(runID)
	wf := compile(t, "apiVersion: orkstack/v1\nkind: Orkflow\nname: run-created-first\nversion: \"1.0.0\"\ntasks:\n"+
		guardChain("c", 3, ""))
	plan, _ := s.tcPlan()
	stub, plan, uri := storeDefinitionIn(t, s.ctx, s.store, runID, wf, plan)
	stub.Version = "2.0.0"
	if err := s.starter.StartRun(s.ctx, runID, stub, nil, plan); err != nil {
		t.Fatalf("StartRun %s: %v", runID, err)
	}
	if status, _ := s.wait(runID, 3*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_FAILED {
		t.Fatalf("the run closed %v, want Failed", status)
	}
	return runID, plan, uri, writes
}

// TestIntegration_RecordRunCreatedFirstHistories records the
// run-created-first replay fixtures from REAL runs on a real cluster:
// generation 0 of a run whose definition passed by reference names another
// version than its stub ends RUN_DEFINITION_CORRUPT through endBeforeStart,
// and the replica's store refuses RUN_CREATED's first write — every attempt
// of its local activity. The gated fixture is recorded by today's code: the
// lazy marker, RUN_CREATED written again under sequence 1, then RUN_FAILED
// at 2. The gap one is recorded by today's code with both of RUN_CREATED's
// writes refused: the marker, two failed writes, then RUN_FAILED at 2, whose
// archive records 1 as a gap. The legacy one is recorded with the gate off
// (DisableRunCreatedFirstGateForTest): no marker, and RUN_FAILED at 2 with
// nothing at 1, the command sequence of every build before the gate. It
// never skips: by default the recordings land under ORK_PROOF_DIR and the
// committed fixtures are left alone; only ORK_RECORD_GOLDEN_HISTORY=1 also
// writes them. It FAILS unless each recording ends Failed with the code,
// holds the stream its path promises, and replays against current code.
func TestIntegration_RecordRunCreatedFirstHistories(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	for _, f := range runCreatedFirstFixtures {
		rec := runCreatedFirstRecordings[f.name]
		path, legacy := f.path, f.legacy
		t.Run(strings.TrimSuffix(filepath.Base(path), ".json"), func(t *testing.T) {
			if legacy {
				defer engine.DisableRunCreatedFirstGateForTest()()
			}
			s := newChaosStack(t, ctx, "run-created-first-fixture")
			runID, plan, uri, writes := s.corruptStart(t, func(n int, _ int64) bool { return n >= 1 && n <= rec.refused })
			recorded := s.history(runID, "")
			if got := failureType(t, recorded); got != engine.RunDefinitionCorruptCode {
				t.Fatalf("the recorded run failed with type %q, want %s", got, engine.RunDefinitionCorruptCode)
			}
			if markers := versionMarkers(t, recorded); legacy != (markers[engine.RunCreatedFirstChange] == 0) {
				t.Fatalf("legacy=%v but markers = %v", legacy, markers)
			}
			want := rec.want
			published := publishedOf(t, recorded)
			if got := fmt.Sprint(definitionEventTypes(published)); got != want {
				t.Fatalf("the recording published %s, want %s", got, want)
			}
			if n, _, _ := writes.counts(); n != rec.writes {
				t.Fatalf("RUN_CREATED's writes began %d times, want %d", n, rec.writes)
			}
			if err := replayRecorded(recorded, runID); err != nil {
				t.Fatalf("the history just recorded does not replay against current code: %v", err)
			}
			// The definition object beside the history, byte for byte: what
			// the fixture's stub names (fixtureObjectDefinition).
			object, err := s.store.ReadSnapshot(ctx, uri)
			if err != nil || artifacts.ContinuationSHA256(object) != plan.Definition.SHA256 {
				t.Fatalf("read the definition back from the store: %v", err)
			}
			objectPath := filepath.Join(strings.TrimSuffix(path, ".json")+".objects", definitionRefObject)
			files := map[string][]byte{path: encodeHistory(t, recorded), objectPath: object}
			write := os.Getenv("ORK_RECORD_GOLDEN_HISTORY") != ""
			written := "the proof directory only (ORK_RECORD_GOLDEN_HISTORY unset)"
			if write {
				written = path + " and its definition object"
			}
			for p, data := range files {
				proof.Write(t, "run-created-first", filepath.Base(filepath.Dir(p))+"-"+filepath.Base(p), data)
				if !write {
					continue
				}
				if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(p, data, 0o644); err != nil {
					t.Fatalf("write %s: %v", p, err)
				}
			}
			t.Logf("recorded %d events of run %s into %s, replayed clean; published %s",
				len(recorded.GetEvents()), runID, written, want)
		})
	}
}

// TestIntegration_ARefusedRunCreatedReadsBackTheDefinitionCode is the
// RUN_DEFINITION_* half of C36's cut store: a run whose definition cannot be
// used, ending RUN_DEFINITION_CORRUPT, with its RUN_CREATED refused twice.
// With the terminal event written, the API serves RUN_FAILED at 2, GetRun
// reads the code and its category from it, and the archive holds it with 1
// recorded as a gap. With the terminal write refused too, nothing is
// published: GetRun and the stream read the code from the execution's close,
// at 1, and nothing archives it, as for any run that publishes no terminal
// event.
func TestIntegration_ARefusedRunCreatedReadsBackTheDefinitionCode(t *testing.T) {
	for _, tc := range []struct {
		name     string
		refuse   func(n int, sequence int64) bool
		want     string
		terminal bool
	}{
		{"the terminal event written", func(n int, _ int64) bool { return n >= 1 }, "[2:RUN_FAILED]", true},
		{"every write refused", func(int, int64) bool { return true }, "[1:RUN_FAILED]", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			s := newChaosStack(t, ctx, "run-created-first-readback")
			runID, _, _, writes := s.corruptStart(t, tc.refuse)
			if n, _, others := writes.counts(); n != 2 || others == 0 {
				t.Fatalf("RUN_CREATED's writes began %d times and %d attempts were at other numbers, want 2 and the "+
					"terminal event attempted", n, others)
			}
			a := s.serveAPI(api.Config{})
			served := apiEvents(t, ctx, a, runID)
			if got := fmt.Sprint(definitionEventTypes(served)); got != tc.want {
				t.Fatalf("StreamRunEvents served %s, want %s", got, tc.want)
			}
			last := served[len(served)-1]
			if !strings.Contains(last.GetMessage(), "not the definition") && !strings.Contains(last.GetMessage(), "another orkflow") {
				t.Fatalf("the served terminal event says %q, want the definition's failure", last.GetMessage())
			}
			run, err := s.reader.GetRun(ctx, runID)
			if err != nil || run.GetStatus() != orkstackv1.Status_STATUS_FAILED ||
				run.GetError().GetCode() != engine.RunDefinitionCorruptCode ||
				run.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION {
				t.Fatalf("GetRun = %v (%v), want FAILED %s ContractViolation", run.GetError(), err, engine.RunDefinitionCorruptCode)
			}
			if !tc.terminal {
				if _, err := s.archive.LoadRun(ctx, runID); !errors.Is(err, artifacts.ErrNotFound) {
					t.Fatalf("the archive body = %v, want none: nothing published a terminal event", err)
				}
				return
			}
			s.requireArchivedWith(t, a, runID, served, engine.RunDefinitionCorruptCode, true)
			t.Logf("%s: StreamRunEvents served %s; GetRun %s; archived with 1 as a gap", tc.name, tc.want,
				run.GetError().GetCode())
		})
	}
}
