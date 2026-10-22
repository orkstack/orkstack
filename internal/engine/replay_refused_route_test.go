package engine_test

import (
	"os"
	"testing"

	"github.com/orkstack/orkstack/internal/engine"
)

// refusedRouteFixtures are the recorded runs of a task whose pool the run's
// plan cannot route (TestIntegration_RecordRefusedRouteHistories,
// testdata/history/README.md). Each shape is recorded twice:
//
//   - gated: by today's code, so it carries the lazy refused-route-task-failed
//     marker and the TASK_FAILED the new path emits;
//   - legacy: with the gate off, which is the command sequence of every build
//     before it — the row left to the run's failure path, which wrote a
//     TASK_LOG cancel for a PENDING row (a pin's opening) and, with a failure
//     handler, recorded a RUNNING one itself.
//
// Every legacy history must keep replaying unchanged (the DefaultVersion
// branch); every gated one must replay only because of the gate.
var refusedRouteFixtures = []struct {
	shape string
	// document is the Orkflow the recorder starts (the same text the workflow
	// tests use).
	document string
}{
	{shape: "plain", document: refusedRoutePlainDocument},
	{shape: "pinned", document: refusedRoutePinnedDocument},
	{shape: "handler", document: refusedRouteHandlerDocument},
}

func refusedRouteFixture(shape string, legacy bool) string {
	if legacy {
		return "testdata/history/run-refused-route-legacy-" + shape + "-v1.json"
	}
	return "testdata/history/run-refused-route-" + shape + "-v1.json"
}

func requireRefusedRouteFixture(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		const hint = "record it with ORK_RECORD_GOLDEN_HISTORY=1 go test -tags=integration ./internal/engine/ " +
			"-run TestIntegration_RecordRefusedRouteHistories"
		if os.Getenv("ORK_REQUIRE_REFUSED_ROUTE_FIXTURE") != "" {
			t.Fatalf("the refused-route fixture %s is missing (%v); %s", path, err, hint)
		}
		t.Skipf("%s is not recorded (%v): %s", path, err, hint)
	}
}

// TestRunWorkflow_ReplaysCommittedRefusedRouteHistories is the command half,
// for both generations of every shape: a history recorded before the gate
// replays its DefaultVersion branch with the command sequence it was
// recorded with (the handler and no-handler variants, and the pin's
// opening), and a history recorded with the gate replays its new path.
func TestRunWorkflow_ReplaysCommittedRefusedRouteHistories(t *testing.T) {
	for _, f := range refusedRouteFixtures {
		for _, legacy := range []bool{true, false} {
			path := refusedRouteFixture(f.shape, legacy)
			name := f.shape + "/gated"
			if legacy {
				name = f.shape + "/legacy"
			}
			t.Run(name, func(t *testing.T) {
				requireRefusedRouteFixture(t, path)
				recorded := loadHistoryFile(t, path)
				runID, _, _, _, _ := startArgs(t, recorded)
				if err := replayAs(t, recorded, runID); err != nil {
					t.Fatalf("replaying %s against current code failed: %v", path, err)
				}
				markers := versionMarkers(t, recorded)[engine.RefusedRouteChange]
				if legacy && markers != 0 {
					t.Fatalf("%s records a %s marker: it is not a history from before the gate", path, engine.RefusedRouteChange)
				}
				if !legacy && markers == 0 {
					t.Fatalf("%s records no %s marker: it does not exercise the gated path", path, engine.RefusedRouteChange)
				}
			})
		}
	}
}

// TestRefusedRouteHistories_CatchTheGateRemoved is the negative test: with the
// gate skipped — the new path taken unconditionally, or a build that never
// asked — the gated fixtures must fail to replay, so they pin the marker and
// the TASK_FAILED it brings.
func TestRefusedRouteHistories_CatchTheGateRemoved(t *testing.T) {
	for _, f := range refusedRouteFixtures {
		if f.shape == "handler" {
			// With a failure handler the old path already recorded the
			// refused task through recordRefusedDispatch (TASK_FAILED,
			// closePinFor), so the command stream is the same with or
			// without the gate; only the marker differs, and the SDK does
			// not reject a marker the code does not ask for. The command
			// half above still replays both generations.
			continue
		}
		t.Run(f.shape, func(t *testing.T) {
			path := refusedRouteFixture(f.shape, false)
			requireRefusedRouteFixture(t, path)
			recorded := loadHistoryFile(t, path)
			runID, _, _, _, _ := startArgs(t, recorded)
			restore := engine.DisableRefusedRouteGateForTest()
			defer restore()
			if err := replayAs(t, recorded, runID); err == nil {
				t.Fatalf("%s replayed with the gate removed: it does not pin the path", path)
			}
		})
	}
}
