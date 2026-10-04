package telemetry

import (
	"bytes"
	"context"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// A composed task has no attempt — it starts a child run — so its terminal
// event records no ork.task.attempts observation, while its count and
// duration (the child's lifetime) are recorded as for any task.
func TestEngineObserver_ComposedTaskRecordsNoAttempts(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	var buf bytes.Buffer
	observer := newTestEngineObserver(t, reader, &buf, false)
	start := time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)
	row := &orkstackv1.TaskRun{
		TaskId: "release", Status: orkstackv1.Status_STATUS_SUCCEEDED,
		ScheduledAt: timestamppb.New(start), FinishedAt: timestamppb.New(start.Add(5 * time.Second)),
		Child: &orkstackv1.TaskChild{OrkflowName: "release-block", OrkflowVersion: "2.1.0", RunId: "ork-1-2"},
	}
	run := &orkstackv1.Run{Id: "run-composed", Namespace: "default", TaskRuns: []*orkstackv1.TaskRun{row}}
	observer.ObserveEvent(context.Background(),
		runEvent("run-composed", orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED, orkstackv1.Status_STATUS_SUCCEEDED, "release"), run)
	var attempts, durations uint64
	var tasks int64
	for _, sm := range collect(t, reader).ScopeMetrics {
		for _, m := range sm.Metrics {
			switch m.Name {
			case "ork.task.attempts":
				if hist, ok := m.Data.(metricdata.Histogram[int64]); ok {
					for _, dp := range hist.DataPoints {
						attempts += dp.Count
					}
				}
			case "ork.task.duration":
				if hist, ok := m.Data.(metricdata.Histogram[float64]); ok {
					for _, dp := range hist.DataPoints {
						durations += dp.Count
					}
				}
			case "ork.tasks":
				if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
					for _, dp := range sum.DataPoints {
						tasks += dp.Value
					}
				}
			}
		}
	}
	if attempts != 0 {
		t.Fatalf("ork.task.attempts recorded %d observations for a composed task, want 0", attempts)
	}
	if durations != 1 || tasks != 1 {
		t.Fatalf("duration observations %d, task count %d; want 1 and 1", durations, tasks)
	}
}
