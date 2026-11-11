package engine_test

import (
	"os"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/engine"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// runCreatedFirstFixtures are the recorded endings before start whose
// RUN_CREATED write failed (TestIntegration_RecordRunCreatedFirstHistories,
// testdata/history/README.md):
//
//   - gated: by today's code, RUN_CREATED's first write failed and the
//     second took: the lazy run-created-first marker, RUN_CREATED written
//     again under sequence 1, then the terminal event at 2;
//   - gap: by today's code, both RUN_CREATED writes failed: the marker, two
//     failed writes, then the terminal event at 2, whose archive records 1
//     as a gap;
//   - legacy: with the gate off, which is the command sequence of every
//     build before it — the terminal event written at sequence 2 right after
//     the failed write.
//
// The legacy history must keep replaying unchanged (the DefaultVersion
// branch); the gated ones replay their new path.
var runCreatedFirstFixtures = []struct {
	name, path string
	legacy     bool
}{
	{"gated", "testdata/history/run-created-first-v1.json", false},
	{"gap", "testdata/history/run-created-first-gap-v1.json", false},
	{"legacy", "testdata/history/run-created-first-legacy-v1.json", true},
}

func requireRunCreatedFirstFixture(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the committed run-created-first fixture %s is missing (%v); record it with "+
			"ORK_RECORD_GOLDEN_HISTORY=1 go test -tags=integration ./internal/engine/ "+
			"-run TestIntegration_RecordRunCreatedFirstHistories", path, err)
	}
}

// TestRunWorkflow_ReplaysCommittedRunCreatedFirstHistories is the command
// half: a history recorded before the gate replays its DefaultVersion branch
// with the command sequence it was recorded with, and a history recorded
// with the gate replays its new path.
func TestRunWorkflow_ReplaysCommittedRunCreatedFirstHistories(t *testing.T) {
	for _, f := range runCreatedFirstFixtures {
		t.Run(f.name, func(t *testing.T) {
			requireRunCreatedFirstFixture(t, f.path)
			recorded := loadHistoryFile(t, f.path)
			runID, _, _, _, _ := startArgs(t, recorded)
			must.OKf(t, replayAs(t, recorded, runID), "replaying %s against current code failed", f.path)
			markers := versionMarkers(t, recorded)[engine.RunCreatedFirstChange]
			if f.legacy && markers != 0 {
				t.Fatalf("%s records a %s marker: it is not a history from before the gate", f.path, engine.RunCreatedFirstChange)
			}
			if !f.legacy && markers == 0 {
				t.Fatalf("%s records no %s marker: it does not exercise the gated path", f.path, engine.RunCreatedFirstChange)
			}
		})
	}
}

// TestRunCreatedFirstHistories_CatchTheGateRemoved is the negative test: with
// the gate skipped — a build that never asked — every gated fixture must fail
// to replay as nondeterministic, so each pins the marker and the second
// RUN_CREATED write it brings.
func TestRunCreatedFirstHistories_CatchTheGateRemoved(t *testing.T) {
	restore := engine.DisableRunCreatedFirstGateForTest()
	defer restore()
	for _, f := range runCreatedFirstFixtures {
		if f.legacy {
			continue
		}
		t.Run(f.name, func(t *testing.T) {
			requireRunCreatedFirstFixture(t, f.path)
			recorded := loadHistoryFile(t, f.path)
			runID, _, _, _, _ := startArgs(t, recorded)
			err := replayAs(t, recorded, runID)
			if err == nil || !strings.Contains(err.Error(), "TMPRL1100") {
				t.Fatalf("%s replayed with the gate removed = %v, want the nondeterminism error (TMPRL1100)", f.path, err)
			}
			t.Logf("with the gate removed, %s does not replay: %v", f.path, err)
		})
	}
}
