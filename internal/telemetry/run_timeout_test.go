package telemetry

import (
	"bytes"
	"context"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestRunTimeoutCounters: a run its own timeout ended counts once on
// ork.run.timeouts, and once more on ork.run.timeout.hard_ends when its
// failure handlers were stopped at the hard end, each labelled with the
// run's namespace (docs/DEPLOY.md §7, "Run and task metrics"). A run that
// declares a timeout but ended otherwise, or declares none, counts on
// neither.
func TestRunTimeoutCounters(t *testing.T) {
	tests := []struct {
		name               string
		event              orkstackv1.EventType
		status             orkstackv1.Status
		timeout            *orkstackv1.RunTimeout
		timeouts, hardEnds int64
	}{
		{"timed out, handlers finished", orkstackv1.EventType_EVENT_TYPE_RUN_FAILED, orkstackv1.Status_STATUS_FAILED,
			&orkstackv1.RunTimeout{Seconds: 60, TimedOut: true}, 1, 0},
		{"timed out, handlers stopped at the hard end", orkstackv1.EventType_EVENT_TYPE_RUN_FAILED, orkstackv1.Status_STATUS_FAILED,
			&orkstackv1.RunTimeout{Seconds: 60, TimedOut: true, HardEndReached: true}, 1, 1},
		{"failing for another reason, handlers stopped at the hard end", orkstackv1.EventType_EVENT_TYPE_RUN_FAILED, orkstackv1.Status_STATUS_FAILED,
			&orkstackv1.RunTimeout{Seconds: 60, HardEndReached: true}, 0, 1},
		{"succeeded within its timeout", orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, orkstackv1.Status_STATUS_SUCCEEDED,
			&orkstackv1.RunTimeout{Seconds: 60}, 0, 0},
		{"canceled by an operator", orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED, orkstackv1.Status_STATUS_CANCELED,
			&orkstackv1.RunTimeout{Seconds: 60}, 0, 0},
		{"no timeout declared", orkstackv1.EventType_EVENT_TYPE_RUN_FAILED, orkstackv1.Status_STATUS_FAILED, nil, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			var buf bytes.Buffer
			observer := newTestEngineObserver(t, reader, &buf, false)
			run := &orkstackv1.Run{Id: "run-timeout", Namespace: "payments", Status: tt.status, Timeout: tt.timeout}
			observer.ObserveEvent(context.Background(), runEvent("run-timeout", tt.event, tt.status, ""), run)
			for name, want := range map[string]int64{RunTimeoutsMetric: tt.timeouts, RunTimeoutHardEndsMetric: tt.hardEnds} {
				points := int64SumPoints(t, reader, name)
				var total int64
				for _, p := range points {
					total += p.Value
					if v, ok := p.Attributes.Value(AttrNamespace); !ok || v.AsString() != "payments" {
						t.Errorf("%s point %v lacks %s=payments", name, p.Attributes, AttrNamespace)
					}
				}
				if total != want {
					t.Errorf("%s = %d, want %d", name, total, want)
				}
			}
		})
	}
}
