package api_test

import (
	"encoding/json/v2"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/stablejson"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestSubmitRun_ThePlanPinsTheLongestRetryWait: a run starts with the longest
// wait between two attempts of a retried task in its plan, in whole seconds
// and always, so a release that changes the engine's default moves only new
// runs. The workflow reads the plan's number, never the engine's constant,
// when it spaces a retry.
func TestSubmitRun_ThePlanPinsTheLongestRetryWait(t *testing.T) {
	orkflows := engine.NewMemoryOrkflowStore()
	starter := &fakeStarter{}
	svc := api.NewService(api.Config{Orkflows: orkflows, Starter: starter})
	must.OK(t, applyDoc(t, svc, retriedDoc("{maximum_attempts: 3}")))
	must.OKf(t, submitName(svc, "retried"), "submit")
	if len(starter.plans) != 1 {
		t.Fatalf("%d runs started, want 1", len(starter.plans))
	}
	plan := starter.plans[0]
	want := int32(engine.DefaultRetryMaxInterval / time.Second)
	if want <= 0 || plan.RetryMaxIntervalSeconds != want {
		t.Fatalf("the plan carries a longest retry wait of %d s, want the engine's default %d s", plan.RetryMaxIntervalSeconds, want)
	}

	// The plan travels as JSON in the workflow's start payload, so the number
	// must be on the wire under the name recorded histories carry it by.
	encoded, err := stablejson.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if got, _ := decoded["retry_max_interval_seconds"].(float64); int32(got) != want {
		t.Fatalf("the encoded plan holds retry_max_interval_seconds = %v, want %d", decoded["retry_max_interval_seconds"], want)
	}
}

// TestSubmitRun_ThePlanPinsTheInterruptionBudgets: every run starts with the
// deployment's interruption budgets in its plan (E92), the defaults when the
// engine sets none and the engine's own when it does, under the name the
// plan carries them by on the wire, so a later release that changes a
// default moves only new runs (AGENTS.md §7.2).
func TestSubmitRun_ThePlanPinsTheInterruptionBudgets(t *testing.T) {
	configured := engine.InterruptionBudgets{Lost: 1, LostRunCap: 5, HandedBack: 2, NeverStarted: 4, NeverStartedFaults: 3, NeverStartedBackoffMillis: 1000}
	for _, tc := range []struct {
		name string
		set  *engine.InterruptionBudgets
		want engine.InterruptionBudgets
	}{
		{"the defaults", nil, engine.DefaultInterruptionBudgets},
		{"the engine's own", &configured, configured},
	} {
		t.Run(tc.name, func(t *testing.T) {
			orkflows := engine.NewMemoryOrkflowStore()
			starter := &fakeStarter{}
			svc := api.NewService(api.Config{Orkflows: orkflows, Starter: starter, Interruptions: tc.set})
			must.OK(t, applyDoc(t, svc, retriedDoc("{maximum_attempts: 3}")))
			must.OKf(t, submitName(svc, "retried"), "submit")
			if len(starter.plans) != 1 {
				t.Fatalf("%d runs started, want 1", len(starter.plans))
			}
			got := starter.plans[0].Interruptions
			if got == nil || *got != tc.want {
				t.Fatalf("the plan pins %+v, want %+v", got, tc.want)
			}
			encoded, err := stablejson.Marshal(starter.plans[0])
			if err != nil {
				t.Fatal(err)
			}
			var decoded map[string]any
			if err := json.Unmarshal(encoded, &decoded); err != nil {
				t.Fatal(err)
			}
			wire, _ := decoded["interruptions"].(map[string]any)
			if wire == nil || int32(wire["handed_back"].(float64)) != tc.want.HandedBack {
				t.Fatalf("the plan's wire form carries interruptions %v", decoded["interruptions"])
			}
		})
	}
}
