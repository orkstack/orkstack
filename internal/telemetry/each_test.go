package telemetry

import (
	"bytes"
	"context"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// A fan-out's node event records ork.task.attempts once per item — each
// item's own attempt — and never with the node's unset attempt.
func TestEngineObserver_RecordsAttemptsOncePerItem(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	var buf bytes.Buffer
	observer := newTestEngineObserver(t, reader, &buf, false)
	node := &orkstackv1.TaskRun{
		TaskId: "convert", Status: orkstackv1.Status_STATUS_SUCCEEDED,
		Each: &orkstackv1.TaskEach{Expanded: true, Count: 3},
		Items: []*orkstackv1.TaskRun{
			{TaskId: "convert", Item: proto.Int32(0), Attempt: 1},
			{TaskId: "convert", Item: proto.Int32(1), Attempt: 2},
			{TaskId: "convert", Item: proto.Int32(2), Attempt: 1},
		},
	}
	run := &orkstackv1.Run{Id: "run-each", Namespace: "default", TaskRuns: []*orkstackv1.TaskRun{node}}
	observer.ObserveEvent(context.Background(),
		runEvent("run-each", orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED, orkstackv1.Status_STATUS_SUCCEEDED, "convert"), run)
	var count uint64
	var sum int64
	for _, sm := range collect(t, reader).ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "ork.task.attempts" {
				continue
			}
			if hist, ok := m.Data.(metricdata.Histogram[int64]); ok {
				for _, dp := range hist.DataPoints {
					count += dp.Count
					sum += dp.Sum
				}
			}
		}
	}
	if count != 3 || sum != 4 {
		t.Fatalf("ork.task.attempts recorded %d observations summing %d, want 3 summing 4", count, sum)
	}
}
