package orker

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"

	"github.com/orkstack/orkstack/internal/task"
)

// TestHeartbeater_RecordsProgressThroughTemporalActivityLifecycle proves the
// production loop reaches Temporal's activity heartbeat sink. Testing beat as
// a plain callback would miss the activity-context guard in NewHeartbeater,
// which is the integration that makes Temporal cancellation observable.
func TestHeartbeater_RecordsProgressThroughTemporalActivityLifecycle(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	observed := make(chan HeartbeatProgress, 1)
	env.SetOnActivityHeartbeatListener(func(_ *activity.Info, details converter.EncodedValues) {
		var progress HeartbeatProgress
		if err := details.Get(&progress); err != nil {
			t.Errorf("decode heartbeat detail: %v", err)
			return
		}
		observed <- progress
	})

	activityFn := func(ctx context.Context) (HeartbeatProgress, error) {
		heartbeat := NewHeartbeater(ctx)
		heartbeat.Phase("container")
		heartbeat.Container(ContainerID("container-proof"))
		heartbeat.Logged(123)
		heartbeat.Start(ctx)
		defer heartbeat.Stop()
		select {
		case progress := <-observed:
			return progress, nil
		case <-time.After(2 * time.Second):
			return HeartbeatProgress{}, fmt.Errorf("heartbeat did not reach Temporal's activity listener")
		}
	}

	env.RegisterActivity(activityFn)
	encoded, err := env.ExecuteActivity(activityFn)
	if err != nil {
		t.Fatalf("ExecuteActivity: %v", err)
	}
	var progress HeartbeatProgress
	if err := encoded.Get(&progress); err != nil {
		t.Fatalf("decode activity result: %v", err)
	}
	if progress.Phase != "container" || progress.ContainerID != "container-proof" || progress.BytesLogged != 123 {
		t.Fatalf("heartbeat detail = %+v, want the current container progress", progress)
	}
	t.Logf("Temporal activity heartbeat received: phase=%s container_id=%s bytes_logged=%d",
		progress.Phase, progress.ContainerID, progress.BytesLogged)
}

// TestHeartbeatInterval_StaysInsideTheDeadWorkerMargin pins the two numbers a
// reader of ORK_ORKER_HEARTBEAT_INTERVAL is choosing between, and the fact
// that the shipped default sits inside them.
//
// The upper bound is the one with teeth: tick more slowly than a third of
// HeartbeatTimeout and Temporal declares LIVE activities dead, which looks
// like random task failures on a healthy cluster and points at nothing.
func TestHeartbeatInterval_StaysInsideTheDeadWorkerMargin(t *testing.T) {
	if DefaultHeartbeatInterval < MinHeartbeatInterval || DefaultHeartbeatInterval > MaxHeartbeatInterval {
		t.Fatalf("DefaultHeartbeatInterval = %v, outside the honorable range [%v, %v]",
			DefaultHeartbeatInterval, MinHeartbeatInterval, MaxHeartbeatInterval)
	}
	if MaxHeartbeatInterval*3 > HeartbeatTimeout {
		t.Errorf("the slowest tick this Orker accepts is %v inside a %v timeout; two missed "+
			"heartbeats would outlast the dead-worker window", MaxHeartbeatInterval, HeartbeatTimeout)
	}
	if MaxHeartbeatInterval != task.MaxHeartbeatInterval {
		t.Errorf("MaxHeartbeatInterval = %v, want the seam's %v — two spellings of one contract "+
			"is one too many", MaxHeartbeatInterval, task.MaxHeartbeatInterval)
	}
}

// TestValidateHeartbeatInterval refuses what it cannot honor, and says which
// variable carried it. A clamped-in-silence value would leave the boot log
// disagreeing with the running process.
func TestValidateHeartbeatInterval(t *testing.T) {
	cases := []struct {
		name    string
		in      time.Duration
		wantErr bool
	}{
		{"zero means the default", 0, false},
		{"the floor itself", MinHeartbeatInterval, false},
		{"the shipped default", DefaultHeartbeatInterval, false},
		{"the ceiling itself", MaxHeartbeatInterval, false},
		{"below the SDK's RPC floor", 500 * time.Millisecond, true},
		{"past the dead-worker margin", MaxHeartbeatInterval + time.Second, true},
		{"a whole minute", time.Minute, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateHeartbeatInterval(tc.in)
			if tc.wantErr && err == nil {
				t.Fatalf("ValidateHeartbeatInterval(%v) = nil, want a refusal", tc.in)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("ValidateHeartbeatInterval(%v) = %v, want accepted", tc.in, err)
			}
			if err != nil && !strings.Contains(err.Error(), HeartbeatIntervalVar) {
				t.Errorf("refusal %q does not name %s; an operator cannot act on it",
					err, HeartbeatIntervalVar)
			}
		})
	}
}

// TestStart_TicksAtTheConfiguredInterval proves the field is actually the loop's
// rate, not decoration: it is what an operator's setting reaches, and the
// number cmd/orker must give the SDK's throttle cap as well.
func TestStart_TicksAtTheConfiguredInterval(t *testing.T) {
	beats := make(chan time.Time, 8)
	h := &Heartbeater{
		beat:     func(HeartbeatProgress) { beats <- time.Now() },
		interval: 20 * time.Millisecond,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.Start(ctx)
	defer h.Stop()

	// One immediate beat, then ticks. Three of them bound two intervals, so a
	// loop that fell back to the 5 s default cannot reach this deadline.
	var seen []time.Time
	for len(seen) < 3 {
		select {
		case at := <-beats:
			seen = append(seen, at)
		case <-time.After(2 * time.Second):
			t.Fatalf("saw %d beats in 2s at a 20ms interval; the configured tick is not the loop's rate", len(seen))
		}
	}
	t.Logf("three beats spanned %v at a 20ms tick", seen[2].Sub(seen[0]))
}

// TestNewActivitiesWithOptions_CarriesTheHeartbeatIntervalToTheDispatcher
// closes the last gap in the chain from the environment to the loop: an
// Options field nothing reads is a setting that silently does nothing.
func TestNewActivitiesWithOptions_CarriesTheHeartbeatIntervalToTheDispatcher(t *testing.T) {
	// No store and no container runtime: NewActivitiesWithOptions does no I/O,
	// and the field under test is carried by the constructor, not by a run.
	activities := NewActivitiesWithOptions(nil, Options{
		WorkRoot:          t.TempDir(),
		HeartbeatInterval: 3 * time.Second,
	})
	if got := activities.Dispatcher().HeartbeatInterval; got != 3*time.Second {
		t.Fatalf("dispatcher heartbeat interval = %v, want the 3s the options asked for", got)
	}
}
