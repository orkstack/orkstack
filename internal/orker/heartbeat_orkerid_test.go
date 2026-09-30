package orker

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"

	"github.com/orkstack/orkstack/internal/task"
)

// O3: the Orker's detail IS the shared contract type the engine decodes.
// A second, drifting declaration in this package would stop compiling here.
var _ task.HeartbeatProgress = HeartbeatProgress{}

// TestHeartbeater_StampsTheOrkerIDOnEveryBeat (O1) proves the self-name rides
// on the first beat and every later one, and that an Orker with no id sends
// the detail without the key at all (omitempty), which is what an old
// Orker's beat looks like to a new engine.
func TestHeartbeater_StampsTheOrkerIDOnEveryBeat(t *testing.T) {
	beats := make(chan HeartbeatProgress, 16)
	h := &Heartbeater{
		beat:     func(p HeartbeatProgress) { beats <- p },
		interval: 10 * time.Millisecond,
		orkerID:  "orker-7",
	}
	h.Phase("run")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.Start(ctx)
	var seen []HeartbeatProgress
	for len(seen) < 3 {
		select {
		case p := <-beats:
			seen = append(seen, p)
		case <-time.After(2 * time.Second):
			t.Fatalf("saw %d beats in 2s at a 10ms tick", len(seen))
		}
	}
	h.Stop()
	for i, p := range seen {
		if p.OrkerID != "orker-7" {
			t.Fatalf("beat %d orker_id = %q, want %q", i, p.OrkerID, "orker-7")
		}
	}
	first, err := json.Marshal(seen[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(first), `"orker_id":"orker-7"`) {
		t.Fatalf("first beat JSON %s lacks the orker_id key", first)
	}
	t.Logf("first beat carried %s; %d beats all stamped orker_id=orker-7", first, len(seen))

	unset := (&Heartbeater{}).progress()
	raw, err := json.Marshal(unset)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "orker_id") {
		t.Fatalf("a beat with no orker id = %s, want the key omitted", raw)
	}
}

// TestDispatcher_HeartbeatsCarryTheDispatcherOrkerID (O2) runs a real
// Execute inside Temporal's activity test environment and decodes every
// heartbeat the SDK hands the listener: each names the dispatcher's OrkerID,
// the same value its result and attempt record carry.
func TestDispatcher_HeartbeatsCarryTheDispatcherOrkerID(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	var mu sync.Mutex
	var observed []HeartbeatProgress
	env.SetOnActivityHeartbeatListener(func(_ *activity.Info, details converter.EncodedValues) {
		var p HeartbeatProgress
		if err := details.Get(&p); err != nil {
			t.Errorf("decode heartbeat detail: %v", err)
			return
		}
		mu.Lock()
		observed = append(observed, p)
		mu.Unlock()
	})

	dispatcher := newPhaseDispatcher(t, &phaseCapturingRuntime{exitCode: 0}, nil)
	dispatcher.OrkerID = "orker-7"
	in := baseExportInput("orker-id-beat")
	activityFn := func(ctx context.Context) error {
		_, err := dispatcher.Execute(ctx, in)
		return err
	}
	env.RegisterActivity(activityFn)
	if _, err := env.ExecuteActivity(activityFn); err != nil {
		t.Fatalf("ExecuteActivity: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(observed) == 0 {
		t.Fatal("no heartbeat reached the activity listener")
	}
	for i, p := range observed {
		if p.OrkerID != "orker-7" {
			t.Fatalf("heartbeat %d = %+v, want orker_id orker-7", i, p)
		}
	}
	t.Logf("%d heartbeats observed, every one with orker_id=orker-7 (first phase %q)", len(observed), observed[0].Phase)
}
