package engine_test

import (
	"os"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/engine"
)

// runCreatedFirstFixtures are the recorded endings before start whose
// RUN_CREATED write failed (TestIntegration_RecordRunCreatedFirstHistories,
// testdata/history/README.md):
//
//   - gated: by today's code, RUN_CREATED's first write failed and the
//     second took: the lazy run-created-first marker, RUN_CREATED written
//     again under sequence 1, then the terminal event at 2;
//   - unwritten: by today's code, both writes failed: the marker, two failed
//     writes, and nothing published;
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
	{"unwritten", "testdata/history/run-created-first-unwritten-v1.json", false},
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
			if err := replayAs(t, recorded, runID); err != nil {
				t.Fatalf("replaying %s against current code failed: %v", f.path, err)
			}
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
// the gate skipped — a build that never asked — the gated fixture must fail
// to replay as nondeterministic, so it pins the marker and the second
// RUN_CREATED write it brings. The unwritten fixture is not here: the old
// path's terminal write takes the place of the second RUN_CREATED write, so
// its commands are the same two failed writes, only the marker differs, and
// the SDK does not reject a marker the code does not ask for. It still pins
// that nothing follows them: a build that wrote anything after the second
// failure would not replay it.
func TestRunCreatedFirstHistories_CatchTheGateRemoved(t *testing.T) {
	path := runCreatedFirstFixtures[0].path
	requireRunCreatedFirstFixture(t, path)
	recorded := loadHistoryFile(t, path)
	runID, _, _, _, _ := startArgs(t, recorded)
	restore := engine.DisableRunCreatedFirstGateForTest()
	defer restore()
	err := replayAs(t, recorded, runID)
	if err == nil || !strings.Contains(err.Error(), "TMPRL1100") {
		t.Fatalf("%s replayed with the gate removed = %v, want the nondeterminism error (TMPRL1100)", path, err)
	}
	t.Logf("with the gate removed, %s does not replay: %v", path, err)
}
