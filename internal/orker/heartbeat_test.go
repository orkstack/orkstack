package orker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
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
