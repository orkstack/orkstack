package engine_test

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/internal/engine"
)

// gateFixtures and censusFixtures are E88 P2's replay fixtures (decision
// G12): each a real execution of the gate or the census on a real cluster,
// recorded by TestIntegration_RecordConcurrencyGateHistories and
// TestIntegration_RecordConcurrencyCensusHistories. The run, resume, task,
// child and fire fixtures come with P3 and later, reset's with P5, and
// cancel_oldest's and replace's with P8.
var (
	gateFixtures = []string{
		"concurrency-gate-basic-v1.json",
		"concurrency-gate-cold-clean-v1.json",
		"concurrency-gate-cold-absent-v1.json",
		"concurrency-gate-verify-v1.json",
		"concurrency-gate-sweep-v1.json",
		"concurrency-gate-task-confirm-v1.json",
		"concurrency-gate-slot-tombstones-v1.json",
		"concurrency-gate-release-below-v1.json",
		"concurrency-gate-stale-incarnation-v1.json",
		"concurrency-gate-reassert-place-v1.json",
		"concurrency-gate-rebuild-v1.json",
		"concurrency-gate-rebuild-failing-v1.json",
		"concurrency-gate-continue-v1.json",
		"concurrency-gate-continue-under-load-v1.json",
		"concurrency-gate-drain-pending-grant-v1.json",
		"concurrency-gate-close-completes-empty-v1.json",
		"concurrency-gate-close-race-v1.json",
		"concurrency-gate-ordinal-replace-v1.json",
		"concurrency-gate-ordinal-late-release-v1.json",
		"concurrency-gate-ordinal-retry-same-n-v1.json",
		"concurrency-gate-ordinal-legacy-tombstone-v1.json",
		"concurrency-gate-grant-without-n-matches-zero-v1.json",
		"concurrency-gate-rebuild-first-v1.json",
		"concurrency-gate-fenced-v1.json",
		"concurrency-gate-freeze-replaced-v1.json",
		"concurrency-gate-freeze-expired-v1.json",
		"concurrency-gate-reset-records-v1.json",
		"concurrency-gate-rebuild-seeded-v1.json",
		"concurrency-gate-reset-lapsed-v1.json",
	}
	censusFixtures = []string{
		"concurrency-census-pass-v1.json",
		"concurrency-census-activity-failure-v1.json",
	}
)

// TestConcurrencyWorkflows_ReplayCommittedHistories replays every committed
// gate and census history against current code. A change to either
// workflow's command sequence that is not gated fails here; the local
// activities' answers replay from their markers, so no cluster is read.
func TestConcurrencyWorkflows_ReplayCommittedHistories(t *testing.T) {
	for _, set := range []struct {
		files    []string
		register func(r worker.WorkflowReplayer)
	}{
		{gateFixtures, func(r worker.WorkflowReplayer) {
			r.RegisterWorkflowWithOptions(engine.ConcurrencyGateWorkflow, workflow.RegisterOptions{Name: engine.ConcurrencyGateWorkflowType})
		}},
		{censusFixtures, func(r worker.WorkflowReplayer) {
			r.RegisterWorkflowWithOptions(engine.ConcurrencyCensusWorkflow, workflow.RegisterOptions{Name: engine.ConcurrencyCensusWorkflowType})
		}},
	} {
		for _, name := range set.files {
			t.Run(name, func(t *testing.T) {
				requireConcurrencyFixture(t, name)
				replayer := worker.NewWorkflowReplayer()
				set.register(replayer)
				if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, filepath.Join("testdata", "history", name)); err != nil {
					t.Fatalf("%s does not replay against current code: %v", name, err)
				}
			})
		}
	}
}

// TestConcurrencyFixtures_EveryOneIsReplayed: every committed concurrency
// fixture is one the replay test reads, so a misnamed recording cannot sit
// beside the one it was meant to replace.
func TestConcurrencyFixtures_EveryOneIsReplayed(t *testing.T) {
	found, err := filepath.Glob(filepath.Join("testdata", "history", "concurrency-*"))
	if err != nil {
		t.Fatal(err)
	}
	listed := map[string]bool{}
	for _, name := range append(append([]string(nil), gateFixtures...), censusFixtures...) {
		listed[name] = true
	}
	for _, f := range concurrencyRunFixtures {
		listed[filepath.Base(f.path())] = true
	}
	for _, f := range concurrencyChildFixtures {
		listed[filepath.Base(f.path())] = true
	}
	listed[filepath.Base(concurrencyResumeFixture)] = true
	for _, path := range found {
		if !listed[filepath.Base(path)] {
			t.Errorf("%s is committed but no replay test reads it", path)
		}
	}
}

// requireConcurrencyFixture skips a fixture not yet recorded, naming the
// command that records it; ORK_REQUIRE_CONCURRENCY_FIXTURES makes its
// absence a failure. Every P2 fixture is committed, so only one a later
// phase adds can be absent, and only until its recording lands.
func requireConcurrencyFixture(t *testing.T, name string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join("testdata", "history", name)); err == nil {
		return
	} else if !errors.Is(err, fs.ErrNotExist) {
		t.Fatal(err)
	}
	const record = "ORK_RECORD_GOLDEN_HISTORY=1 ORK_REQUIRE_INFRA=1 go test -tags=integration -p 1 -count=1 " +
		"./internal/engine/ -run 'TestIntegration_RecordConcurrency'"
	if os.Getenv("ORK_REQUIRE_CONCURRENCY_FIXTURES") != "" {
		t.Fatalf("%s is not recorded; record it with %s", name, record)
	}
	t.Skipf("%s is not recorded yet; record it with %s", name, record)
}
