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
