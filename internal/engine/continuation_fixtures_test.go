package engine_test

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// committedFixtures is every committed history fixture's path.
func committedFixtures(t *testing.T) []string {
	t.Helper()
	paths, err := filepath.Glob("testdata/history/run-*.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no committed fixtures")
	}
	return paths
}

// TestContinuation_RestoreEqualsLiveStateAtEveryIdlePoint: every committed
// fixture's definition, re-executed with its own recorded results and
// resolutions, switches at EVERY natural idle point; at each one the state
// the next generation restored equals the live state the switch saved, field
// by field: rows, the result projections, outputs and their presence,
// fan-out counters, await deadlines (with a timer to cancel), the event
// sequence, created_at and the version decisions.
func TestContinuation_RestoreEqualsLiveStateAtEveryIdlePoint(t *testing.T) {
	defer engine.ContinueAtEveryIdleForTest()()
	for _, path := range committedFixtures(t) {
		if strings.Contains(path, "-continued-") || strings.Contains(path, "in-flight-cap") {
			continue // this stage's own fixtures already continue
		}
		t.Run(filepath.Base(path), func(t *testing.T) {
			recorded := loadHistoryFile(t, path)
			runID, wf, input, plan := goldenWorkflowArgs(t, recorded)
			inputs, results := recordedItemPayloads(t, recorded)
			signals, offsets := recordedResolutions(t, recorded)
			var count int32
			for _, in := range inputs {
				if each, ok := in["each"].(map[string]any); ok {
					if n, ok := each["count"].(float64); ok {
						count = int32(n)
					}
				}
			}
			plan.History = &engine.HistoryLimit{MaxEvents: engine.DefaultHistoryMaxEvents,
				MaxBytes: engine.DefaultHistoryMaxBytes, Reserve: engine.DefaultHistoryReserve}
			budget := engine.DefaultContinuationBudget(1, 1<<40, time.Hour)
			plan.Continuation = &budget

			var mu sync.Mutex
			var switches, restores []engine.DecisionState
			defer engine.ObserveDecisionStatesForTest(func(phase string, state engine.DecisionState) {
				mu.Lock()
				defer mu.Unlock()
				if phase == "switch" {
					switches = append(switches, state)
				} else {
					restores = append(restores, state)
				}
			})()

			var current *harness
			behavior := func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
				result, ok := results[scheduleKey(in)]
				if !ok {
					// A child run's task: the recording holds only the
					// parent's.
					fallback := succeeded(in.GetTaskId(), map[string]any{"value": "v"})
					fallback.ValuesRef.Uri = "mem://values/" + in.GetRunId() + "/" + scheduleKey(in)
					result = fallback
				}
				// The recorded values — or a stand-in where the recording
				// kept none inline — in the harness's store, under a
				// reference to exactly those bytes, so a render reads them.
				result = proto.Clone(result).(*orkstackv1.ExecuteTaskResult)
				values := result.GetValuesJson()
				if len(values) == 0 {
					values = []byte(`{"value":"v"}`)
				}
				if result.GetValuesRef() != nil {
					result.ValuesRef = current.values.put(result.GetValuesRef().GetUri(), values)
					result.ValuesJson, result.ValuesInlined = values, true
				}
				return result, nil
			}
			g := runGenerations(t, runID, wf, input, plan, behavior, func(h *harness, _ int) {
				current = h
				enablePins(h, worker.Options{})
				h.countItems = func(*orkstackv1.ArtifactRef) (int32, error) { return count, nil }
				for i, sig := range signals {
					h.signalAt(offsets[i], sig)
				}
			})
			if len(g.carries) == 0 {
				t.Fatalf("the run never switched (ended %v)", g.err)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(restores) != len(switches) {
				t.Fatalf("%d switches, %d restores", len(switches), len(restores))
			}
			for i := range switches {
				requireDecisionStatesEqual(t, i, switches[i], restores[i])
			}
			t.Logf("%d idle points switched and restored exactly (run ended %v)", len(switches), g.err)
		})
	}
}

func requireDecisionStatesEqual(t *testing.T, i int, live, restored engine.DecisionState) {
	t.Helper()
	if live.EventSeq != restored.EventSeq || live.CreatedAt != restored.CreatedAt {
		t.Fatalf("switch %d: sequence %d/%d created %s/%s", i, live.EventSeq, restored.EventSeq, live.CreatedAt, restored.CreatedAt)
	}
	equal := func(what string, a, b map[string]string) {
		t.Helper()
		if len(a) != len(b) {
			t.Fatalf("switch %d: %s has %d entries live, %d restored:\n%v\n%v", i, what, len(a), len(b), keysOf(a), keysOf(b))
		}
		for k, v := range a {
			if b[k] != v {
				t.Fatalf("switch %d: %s[%s] differs:\nlive     %q\nrestored %q", i, what, k, v, b[k])
			}
		}
	}
	equal("task rows", live.TaskRuns, restored.TaskRuns)
	equal("results", live.Results, restored.Results)
	equal("outputs", live.Outputs, restored.Outputs)
	equal("awaits", live.Awaits, restored.Awaits)
	if len(live.Eaches) != len(restored.Eaches) {
		t.Fatalf("switch %d: fan-outs %v / %v", i, live.Eaches, restored.Eaches)
	}
	for id, counters := range live.Eaches {
		if restored.Eaches[id] != counters {
			t.Fatalf("switch %d: fan-out %s counters live %v restored %v", i, id, counters, restored.Eaches[id])
		}
	}
	if len(live.Versions) != len(restored.Versions) {
		t.Fatalf("switch %d: versions %v / %v", i, live.Versions, restored.Versions)
	}
	for change, v := range live.Versions {
		if restored.Versions[change] != v {
			t.Fatalf("switch %d: version %s live %d restored %d", i, change, v, restored.Versions[change])
		}
	}
	for id, await := range restored.Awaits {
		if !strings.HasSuffix(await, "cancel=true") {
			t.Fatalf("switch %d: restored await %s has no timer to cancel: %s", i, id, await)
		}
	}
}

func keysOf(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
