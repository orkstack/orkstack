package telemetry

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// newTestEngineObserver wires an EngineObserver to a ManualReader (so a test
// can pull the current metric state on demand) and a text-handler logger
// backed by buf, so a test can assert on the log line it produced.
func newTestEngineObserver(t *testing.T, reader sdkmetric.Reader, buf *bytes.Buffer, highCard bool) *EngineObserver {
	t.Helper()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { meterProvider.Shutdown(context.Background()) })
	providers := &Providers{
		Meter: meterProvider,
		cfg:   Config{HighCardinality: highCard},
	}
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	observer, err := NewEngineObserver(providers, logger)
	if err != nil {
		t.Fatalf("NewEngineObserver: %v", err)
	}
	return observer
}

func collect(t *testing.T, reader sdkmetric.Reader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	return rm
}

func runEvent(runID string, eventType orkstackv1.EventType, status orkstackv1.Status, taskID string) *orkstackv1.Event {
	return &orkstackv1.Event{
		RunId:  runID,
		TaskId: taskID,
		Type:   eventType,
		Status: status,
	}
}

func TestObserveEvent_TerminalRunIncrementsCounterAndRecordsDuration(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	var buf bytes.Buffer
	observer := newTestEngineObserver(t, reader, &buf, false)

	started := time.Unix(1755302400, 0).UTC()
	finished := started.Add(42 * time.Second)
	run := &orkstackv1.Run{
		Id:          "run-1",
		Namespace:   "default",
		OrkflowName: "hello-ork",
		Status:      orkstackv1.Status_STATUS_SUCCEEDED,
		StartedAt:   timestamppb.New(started),
		FinishedAt:  timestamppb.New(finished),
	}
	observer.ObserveEvent(context.Background(),
		runEvent("run-1", orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, orkstackv1.Status_STATUS_SUCCEEDED, ""),
		run)

	rm := collect(t, reader)
	found := false
	var gotSeconds float64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "ork.runs" {
				if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
					for _, dp := range sum.DataPoints {
						if v, ok := dp.Attributes.Value(AttrStatus); ok && v.AsString() == "succeeded" {
							found = true
							if dp.Value != 1 {
								t.Errorf("ork.runs value = %d, want 1", dp.Value)
							}
						}
					}
				}
			}
			if m.Name == "ork.run.duration" {
				if hist, ok := m.Data.(metricdata.Histogram[float64]); ok {
					for _, dp := range hist.DataPoints {
						if v, ok := dp.Attributes.Value(AttrStatus); ok && v.AsString() == "succeeded" {
							gotSeconds = dp.Sum
						}
					}
				}
			}
		}
	}
	if !found {
		t.Fatal("ork.runs{status=succeeded} was not recorded")
	}
	if gotSeconds != 42 {
		t.Errorf("ork.run.duration sum = %v, want 42", gotSeconds)
	}
}

func TestObserveEvent_TaskFailedRecordsCategory(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	var buf bytes.Buffer
	observer := newTestEngineObserver(t, reader, &buf, false)

	started := time.Unix(1755302400, 0).UTC()
	finished := started.Add(3 * time.Second)
	run := &orkstackv1.Run{
		Id:        "run-2",
		Namespace: "default",
		TaskRuns: []*orkstackv1.TaskRun{
			{
				TaskId:     "build",
				Pool:       "default",
				Status:     orkstackv1.Status_STATUS_FAILED,
				Attempt:    2,
				StartedAt:  timestamppb.New(started),
				FinishedAt: timestamppb.New(finished),
				Error: &orkstackv1.Error{
					Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED,
				},
			},
		},
	}
	observer.ObserveEvent(context.Background(),
		runEvent("run-2", orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, orkstackv1.Status_STATUS_FAILED, "build"),
		run)

	rm := collect(t, reader)
	var gotCategory string
	var gotAttempt int64
	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "ork.tasks" {
				if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
					for _, dp := range sum.DataPoints {
						if v, ok := dp.Attributes.Value(AttrStatus); ok && v.AsString() == "failed" {
							found = true
							if v, ok := dp.Attributes.Value(AttrFailureCategory); ok {
								gotCategory = v.AsString()
							}
						}
					}
				}
			}
			if m.Name == "ork.task.attempts" {
				if hist, ok := m.Data.(metricdata.Histogram[int64]); ok {
					for _, dp := range hist.DataPoints {
						gotAttempt = dp.Sum
					}
				}
			}
		}
	}
	if !found {
		t.Fatal("ork.tasks{status=failed} was not recorded")
	}
	if gotCategory != "activity_failed" {
		t.Errorf("failure_category = %q, want %q", gotCategory, "activity_failed")
	}
	if gotAttempt != 2 {
		t.Errorf("ork.task.attempts sum = %d, want 2", gotAttempt)
	}
}

func TestObserveEvent_HighCardinalityAddsRunAndTaskAttrs(t *testing.T) {
	for _, tc := range []struct {
		name     string
		highCard bool
	}{
		{"low cardinality omits run_id", false},
		{"high cardinality adds run_id", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			var buf bytes.Buffer
			observer := newTestEngineObserver(t, reader, &buf, tc.highCard)

			run := &orkstackv1.Run{
				Id:        "run-3",
				Namespace: "default",
				Status:    orkstackv1.Status_STATUS_SUCCEEDED,
			}
			observer.ObserveEvent(context.Background(),
				runEvent("run-3", orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, orkstackv1.Status_STATUS_SUCCEEDED, ""),
				run)

			rm := collect(t, reader)
			hasRunID := false
			for _, sm := range rm.ScopeMetrics {
				for _, m := range sm.Metrics {
					if m.Name != "ork.runs" {
						continue
					}
					if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
						for _, dp := range sum.DataPoints {
							if _, ok := dp.Attributes.Value(AttrRunID); ok {
								hasRunID = true
							}
						}
					}
				}
			}
			if hasRunID != tc.highCard {
				t.Errorf("ork.runs carries run_id = %v, want %v", hasRunID, tc.highCard)
			}
		})
	}
}

func TestObserveEvent_MissingTaskDoesNotPanic(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	var buf bytes.Buffer
	observer := newTestEngineObserver(t, reader, &buf, false)

	run := &orkstackv1.Run{Id: "run-4", Namespace: "default"}
	observer.ObserveEvent(context.Background(),
		runEvent("run-4", orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, orkstackv1.Status_STATUS_FAILED, "ghost-task"),
		run)

	rm := collect(t, reader)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "ork.tasks" {
				if sum, ok := m.Data.(metricdata.Sum[int64]); ok && len(sum.DataPoints) != 0 {
					t.Errorf("ork.tasks recorded %d points for a task missing from the run, want 0", len(sum.DataPoints))
				}
			}
		}
	}
}

func TestObserveEvent_NilRunDoesNotPanic(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	var buf bytes.Buffer
	observer := newTestEngineObserver(t, reader, &buf, false)

	observer.ObserveEvent(context.Background(),
		runEvent("run-5", orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, orkstackv1.Status_STATUS_SUCCEEDED, ""),
		nil)
}

func TestObserveEvent_TaskLogLogsAtDebugWithNoMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	var buf bytes.Buffer
	observer := newTestEngineObserver(t, reader, &buf, false)

	run := &orkstackv1.Run{Id: "run-6", Namespace: "default", OrkflowName: "hello-ork"}
	observer.ObserveEvent(context.Background(),
		runEvent("run-6", orkstackv1.EventType_EVENT_TYPE_TASK_LOG, orkstackv1.Status_STATUS_RUNNING, "build"),
		run)

	rm := collect(t, reader)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "ork.tasks" || m.Name == "ork.runs" {
				if sum, ok := m.Data.(metricdata.Sum[int64]); ok && len(sum.DataPoints) != 0 {
					t.Errorf("%s recorded a point for TASK_LOG, want none", m.Name)
				}
			}
		}
	}
	logged := buf.String()
	if !containsAll(logged, "level=DEBUG", "event_type=task_log", "run_id=run-6") {
		t.Errorf("TASK_LOG log line = %q, want level=DEBUG with event_type and run_id", logged)
	}
}

// TestObserveEvent_LogLineCarriesEventTypeAndRunID covers the Info half of
// fix 9's gate: a run event logs at Info when the resolved sink has logs
// switched on.
func TestObserveEvent_LogLineCarriesEventTypeAndRunID(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { meterProvider.Shutdown(context.Background()) })
	providers := &Providers{Meter: meterProvider, cfg: Config{LogsExporter: ExporterOTLP}}
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	observer, err := NewEngineObserver(providers, logger)
	if err != nil {
		t.Fatalf("NewEngineObserver: %v", err)
	}

	run := &orkstackv1.Run{Id: "run-7", Namespace: "default", OrkflowName: "hello-ork"}
	observer.ObserveEvent(context.Background(),
		runEvent("run-7", orkstackv1.EventType_EVENT_TYPE_RUN_STARTED, orkstackv1.Status_STATUS_RUNNING, ""),
		run)

	logged := buf.String()
	if !containsAll(logged, "level=INFO", "event_type=run_started", "run_id=run-7", "status=running") {
		t.Errorf("log line = %q, want event_type, run_id and status", logged)
	}
}

// TestObserveEvent_RunEventLogsAtDebugWhenSinkLogsOff is fix 9's other half:
// a deployment with only metrics on (no LogsExporter) must not gain a
// per-event Info line it never asked for — the same event logs at Debug
// instead of Info.
func TestObserveEvent_RunEventLogsAtDebugWhenSinkLogsOff(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	var buf bytes.Buffer
	// newTestEngineObserver's Providers has no LogsExporter set, so
	// LogsEnabled() is false for the resolved (default) sink.
	observer := newTestEngineObserver(t, reader, &buf, false)

	run := &orkstackv1.Run{Id: "run-8", Namespace: "default", OrkflowName: "hello-ork"}
	observer.ObserveEvent(context.Background(),
		runEvent("run-8", orkstackv1.EventType_EVENT_TYPE_RUN_STARTED, orkstackv1.Status_STATUS_RUNNING, ""),
		run)

	logged := buf.String()
	if !containsAll(logged, "level=DEBUG", "event_type=run_started", "run_id=run-8") {
		t.Errorf("log line = %q, want level=DEBUG (sink logs are off)", logged)
	}
	if strings.Contains(logged, "level=INFO") {
		t.Errorf("log line = %q, want no level=INFO line (sink logs are off)", logged)
	}
}

// runWithSelection returns a Run pinned to sink with the given selection
// attributes.
func runWithSelection(id, sink string, attrs map[string]string) *orkstackv1.Run {
	return &orkstackv1.Run{
		Id:        id,
		Namespace: "default",
		Status:    orkstackv1.Status_STATUS_SUCCEEDED,
		Telemetry: &orkstackv1.TelemetrySelection{Sink: sink, Attributes: attrs},
	}
}

// TestNewEngineObserverForSinks_RunPinnedToNamedSinkLandsThere is the phase 3
// routing contract: a run pinned to sink "b" moves b's ManualReader, not the
// default sink's.
func TestNewEngineObserverForSinks_RunPinnedToNamedSinkLandsThere(t *testing.T) {
	defReader := sdkmetric.NewManualReader()
	defMeter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(defReader))
	t.Cleanup(func() { defMeter.Shutdown(context.Background()) })
	def := &Providers{Meter: defMeter, cfg: Config{MetricsExporters: []string{ExporterOTLP}}}

	bReader := sdkmetric.NewManualReader()
	bMeter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(bReader))
	t.Cleanup(func() { bMeter.Shutdown(context.Background()) })
	b := &Providers{Meter: bMeter, cfg: Config{MetricsExporters: []string{ExporterOTLP}}}

	sinks := newSinksForTest(def, map[string]*Providers{"b": b})
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	observer, err := NewEngineObserverForSinks(sinks, logger)
	if err != nil {
		t.Fatalf("NewEngineObserverForSinks: %v", err)
	}

	run := runWithSelection("run-b", "b", nil)
	observer.ObserveEvent(context.Background(),
		runEvent("run-b", orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, orkstackv1.Status_STATUS_SUCCEEDED, ""),
		run)

	bHasPoint := false
	for _, sm := range collect(t, bReader).ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "ork.runs" {
				if sum, ok := m.Data.(metricdata.Sum[int64]); ok && len(sum.DataPoints) > 0 {
					bHasPoint = true
				}
			}
		}
	}
	if !bHasPoint {
		t.Error("sink b's reader recorded no ork.runs point, want one")
	}
	for _, sm := range collect(t, defReader).ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "ork.runs" {
				if sum, ok := m.Data.(metricdata.Sum[int64]); ok && len(sum.DataPoints) > 0 {
					t.Error("default sink's reader recorded an ork.runs point, want none")
				}
			}
		}
	}
}

// TestNewEngineObserverForSinks_SelectionAttrsOnLogAlwaysOnMetricOnlyHigh
// covers the cardinality trade-off: a run's telemetry selection attributes
// appear on the log line regardless, but only appear on the ork.runs metric
// when that sink's Cardinality is High.
func TestNewEngineObserverForSinks_SelectionAttrsOnLogAlwaysOnMetricOnlyHigh(t *testing.T) {
	for _, tc := range []struct {
		name     string
		highCard bool
	}{
		{"low cardinality: log yes, metric no", false},
		{"high cardinality: log yes, metric yes", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			t.Cleanup(func() { meter.Shutdown(context.Background()) })
			def := &Providers{Meter: meter, cfg: Config{MetricsExporters: []string{ExporterOTLP}, HighCardinality: tc.highCard}}
			sinks := newSinksForTest(def, nil)

			var buf bytes.Buffer
			// This Providers has no LogsExporter set, so the "run event" line
			// logs at Debug (fix 9) — the handler must accept Debug for the
			// line to appear at all; its content is what this test checks.
			logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
			observer, err := NewEngineObserverForSinks(sinks, logger)
			if err != nil {
				t.Fatalf("NewEngineObserverForSinks: %v", err)
			}

			run := runWithSelection("run-attrs", "", map[string]string{"ork.ns.team": "payments"})
			observer.ObserveEvent(context.Background(),
				runEvent("run-attrs", orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, orkstackv1.Status_STATUS_SUCCEEDED, ""),
				run)

			if !containsAll(buf.String(), "ork.ns.team=payments") {
				t.Errorf("log line = %q, want ork.ns.team=payments always", buf.String())
			}

			hasMetricAttr := false
			for _, sm := range collect(t, reader).ScopeMetrics {
				for _, m := range sm.Metrics {
					if m.Name != "ork.runs" {
						continue
					}
					if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
						for _, dp := range sum.DataPoints {
							if _, ok := dp.Attributes.Value(attribute.Key("ork.ns.team")); ok {
								hasMetricAttr = true
							}
						}
					}
				}
			}
			if hasMetricAttr != tc.highCard {
				t.Errorf("ork.runs carries ork.ns.team = %v, want %v (HighCardinality=%v)", hasMetricAttr, tc.highCard, tc.highCard)
			}
		})
	}
}

func containsAll(s string, substrings ...string) bool {
	for _, sub := range substrings {
		if !bytes.Contains([]byte(s), []byte(sub)) {
			return false
		}
	}
	return true
}

// taskDurationSum is the ork.task.duration histogram's sum for one status.
func taskDurationSum(t *testing.T, reader sdkmetric.Reader, status string) (float64, bool) {
	t.Helper()
	for _, sm := range collect(t, reader).ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "ork.task.duration" {
				continue
			}
			if hist, ok := m.Data.(metricdata.Histogram[float64]); ok {
				for _, dp := range hist.DataPoints {
					if v, ok := dp.Attributes.Value(AttrStatus); ok && v.AsString() == status {
						return dp.Sum, true
					}
				}
			}
		}
	}
	return 0, false
}

// TestEngineObserver_TaskDurationMeasuresDispatchToFinish: correcting
// TaskRun.started_at to the Orker's start must not silently change what
// ork.task.duration measures. It stays dispatch to finish — finished_at −
// scheduled_at — and falls back to started_at on a snapshot that has no
// scheduled_at (a pre-contract run, where started_at held the dispatch
// instant).
func TestEngineObserver_TaskDurationMeasuresDispatchToFinish(t *testing.T) {
	scheduled := time.Unix(1755302400, 0).UTC()
	cases := []struct {
		name string
		task *orkstackv1.TaskRun
		want float64
	}{
		{
			name: "scheduled_at set: dispatch to finish, not the Orker's run time",
			task: &orkstackv1.TaskRun{
				TaskId:      "build",
				Status:      orkstackv1.Status_STATUS_SUCCEEDED,
				ScheduledAt: timestamppb.New(scheduled),
				StartedAt:   timestamppb.New(scheduled.Add(30 * time.Second)),
				FinishedAt:  timestamppb.New(scheduled.Add(50 * time.Second)),
			},
			want: 50,
		},
		{
			name: "pre-contract snapshot: started_at is the dispatch instant",
			task: &orkstackv1.TaskRun{
				TaskId:     "build",
				Status:     orkstackv1.Status_STATUS_SUCCEEDED,
				StartedAt:  timestamppb.New(scheduled),
				FinishedAt: timestamppb.New(scheduled.Add(12 * time.Second)),
			},
			want: 12,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			var buf bytes.Buffer
			observer := newTestEngineObserver(t, reader, &buf, false)
			run := &orkstackv1.Run{Id: "run-e8", Namespace: "default", TaskRuns: []*orkstackv1.TaskRun{tc.task}}
			observer.ObserveEvent(context.Background(),
				runEvent("run-e8", orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED, orkstackv1.Status_STATUS_SUCCEEDED, "build"),
				run)
			got, ok := taskDurationSum(t, reader, "succeeded")
			if !ok {
				t.Fatal("ork.task.duration{status=succeeded} was not recorded")
			}
			if got != tc.want {
				t.Fatalf("ork.task.duration = %v, want %v", got, tc.want)
			}
		})
	}
}
