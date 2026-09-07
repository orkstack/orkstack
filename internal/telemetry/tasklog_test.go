package telemetry

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"go.opentelemetry.io/otel/attribute"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func testInput() *orkstackv1.ExecuteTaskInput {
	return &orkstackv1.ExecuteTaskInput{
		RunId:          "r1",
		TaskId:         "t1",
		Namespace:      "ns1",
		OrkflowName:    "of1",
		OrkflowVersion: "v1",
		Pool:           "pool1",
	}
}

func newTestProviders(t *testing.T, exporter sdklog.Exporter, reader sdkmetric.Reader) *Providers {
	t.Helper()
	logProvider := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(exporter)))
	t.Cleanup(func() { logProvider.Shutdown(context.Background()) })
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { meterProvider.Shutdown(context.Background()) })
	return &Providers{
		Logs:  logProvider,
		Meter: meterProvider,
		cfg:   Config{LogsExporter: ExporterOTLP, MetricsExporters: []string{ExporterOTLP}},
	}
}

func TestTaskLogWriterDecodesStreams(t *testing.T) {
	exporter := &recordingExporter{}
	reader := sdkmetric.NewManualReader()
	providers := newTestProviders(t, exporter, reader)

	exp := NewTaskLogExporter(providers, 0)
	if exp == nil {
		t.Fatal("NewTaskLogExporter = nil, want an exporter")
	}
	w := exp.Open(context.Background(), testInput(), 1)
	defer w.Close()

	lines := []string{"[stdout] a", "[stderr] b", "[ork] c", "plain"}
	for _, l := range lines {
		w.Write(l)
	}

	if len(exporter.records) != 4 {
		t.Fatalf("got %d records, want 4", len(exporter.records))
	}
	wantStream := []string{StreamStdout, StreamStderr, StreamOrk, StreamStdout}
	wantBody := []string{"a", "b", "c", "plain"}
	for i, rec := range exporter.records {
		if got := rec.Body().AsString(); got != wantBody[i] {
			t.Errorf("record %d body = %q, want %q", i, got, wantBody[i])
		}
		var stream string
		var runID string
		rec.WalkAttributes(func(kv attribute.KeyValue) bool {
			switch string(kv.Key) {
			case AttrStream:
				stream = kv.Value.AsString()
			case AttrRunID:
				runID = kv.Value.AsString()
			}
			return true
		})
		if stream != wantStream[i] {
			t.Errorf("record %d stream = %q, want %q", i, stream, wantStream[i])
		}
		if runID != "r1" {
			t.Errorf("record %d run_id = %q, want %q", i, runID, "r1")
		}
	}
}

func TestTaskLogWriterTruncatesLongLines(t *testing.T) {
	exporter := &recordingExporter{}
	reader := sdkmetric.NewManualReader()
	providers := newTestProviders(t, exporter, reader)

	exp := NewTaskLogExporter(providers, 4)
	w := exp.Open(context.Background(), testInput(), 1)
	defer w.Close()

	w.Write("hello world")

	if len(exporter.records) != 1 {
		t.Fatalf("got %d records, want 1", len(exporter.records))
	}
	if got := exporter.records[0].Body().AsString(); got != "hell" {
		t.Errorf("body = %q, want truncated to 4 bytes %q", got, "hell")
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	outcome := findCounterOutcome(t, rm, "ork.orker.task_log.records", OutcomeTruncated)
	if outcome != 1 {
		t.Errorf("outcome=truncated count = %d, want 1", outcome)
	}
}

// findCounterOutcome returns the int64 sum recorded for the given metric name
// and outcome= attribute value, or 0 if no matching data point exists.
func findCounterOutcome(t *testing.T, rm metricdata.ResourceMetrics, name, outcome string) int64 {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				if v, ok := dp.Attributes.Value(attribute.Key(AttrOutcome)); ok && v.AsString() == outcome {
					return dp.Value
				}
			}
		}
	}
	return 0
}

func TestNewTaskLogExporterDisabled(t *testing.T) {
	if exp := NewTaskLogExporter(nil, 0); exp != nil {
		t.Errorf("NewTaskLogExporter(nil, 0) = %v, want nil", exp)
	}
	p, err := Setup(context.Background(), Config{})
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if exp := NewTaskLogExporter(p, 0); exp != nil {
		t.Errorf("NewTaskLogExporter on disabled Providers = %v, want nil", exp)
	}
}

func TestNilTaskLogExporterOpenIsSafe(t *testing.T) {
	var exp *TaskLogExporter
	w := exp.Open(context.Background(), testInput(), 1)
	if w != nil {
		t.Fatalf("nil *TaskLogExporter.Open = %v, want nil", w)
	}
	// Every method on a nil *TaskLogWriter must be a no-op, since the Orker
	// holds it behind an interface where a nil pointer is still callable.
	w.Write("should not panic")
	w.Close()
}

func TestTaskLogWriterWriteAfterCloseIsSafe(t *testing.T) {
	exporter := &recordingExporter{}
	reader := sdkmetric.NewManualReader()
	providers := newTestProviders(t, exporter, reader)

	exp := NewTaskLogExporter(providers, 0)
	w := exp.Open(context.Background(), testInput(), 1)
	w.Close()
	w.Write("after close")
	if len(exporter.records) != 0 {
		t.Errorf("got %d records after Close, want 0", len(exporter.records))
	}
}

// testInputForSink returns an ExecuteTaskInput pinned to sink, carrying attrs
// as its telemetry selection's Attributes.
func testInputForSinkWithLogs(sink string, taskLogs orkstackv1.TaskLogExport, attrs map[string]string) *orkstackv1.ExecuteTaskInput {
	in := testInput()
	in.Telemetry = &orkstackv1.TelemetrySelection{
		Sink:       sink,
		TaskLogs:   taskLogs,
		Attributes: attrs,
	}
	return in
}

// TestTaskLogExporterForSinks_GlobalAndSelectionInteract is the table over
// (globalExport, selection.TaskLogs) the phase 3 plan calls for: a
// selection's ENABLED/DISABLED always overrides the deployment default
// either way, and UNSPECIFIED leaves the default in charge.
func TestTaskLogExporterForSinks_GlobalAndSelectionInteract(t *testing.T) {
	for _, tc := range []struct {
		name         string
		globalExport bool
		selection    orkstackv1.TaskLogExport
		wantOpen     bool
	}{
		{"global on, unspecified selection stays on", true, orkstackv1.TaskLogExport_TASK_LOG_EXPORT_UNSPECIFIED, true},
		{"global off, unspecified selection stays off", false, orkstackv1.TaskLogExport_TASK_LOG_EXPORT_UNSPECIFIED, false},
		{"global off, selection enables", false, orkstackv1.TaskLogExport_TASK_LOG_EXPORT_ENABLED, true},
		{"global on, selection disables", true, orkstackv1.TaskLogExport_TASK_LOG_EXPORT_DISABLED, false},
		{"global off, selection disables (stays off)", false, orkstackv1.TaskLogExport_TASK_LOG_EXPORT_DISABLED, false},
		{"global on, selection enables (stays on)", true, orkstackv1.TaskLogExport_TASK_LOG_EXPORT_ENABLED, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exporter := &recordingExporter{}
			reader := sdkmetric.NewManualReader()
			def := newTestProviders(t, exporter, reader)
			sinks := newSinksForTest(def, nil)

			exp := NewTaskLogExporterForSinks(sinks, 0, tc.globalExport)
			if exp == nil {
				t.Fatal("NewTaskLogExporterForSinks = nil, want non-nil always")
			}
			in := testInputForSinkWithLogs("", tc.selection, nil)
			w := exp.Open(context.Background(), in, 1)
			if (w != nil) != tc.wantOpen {
				t.Errorf("Open = %v (nil=%v), want non-nil=%v", w, w == nil, tc.wantOpen)
			}
			if w != nil {
				w.Write("line")
				if len(exporter.records) != 1 {
					t.Errorf("got %d records, want 1", len(exporter.records))
				}
			}
		})
	}
}

// TestTaskLogExporterForSinks_RoutesToNamedSink confirms a selection naming
// sink "b" lands its lines in b's exporter and never in the default's.
func TestTaskLogExporterForSinks_RoutesToNamedSink(t *testing.T) {
	defExporter := &recordingExporter{}
	defReader := sdkmetric.NewManualReader()
	def := newTestProviders(t, defExporter, defReader)

	bExporter := &recordingExporter{}
	bReader := sdkmetric.NewManualReader()
	b := newTestProviders(t, bExporter, bReader)

	sinks := newSinksForTest(def, map[string]*Providers{"b": b})
	exp := NewTaskLogExporterForSinks(sinks, 0, true)

	in := testInputForSinkWithLogs("b", orkstackv1.TaskLogExport_TASK_LOG_EXPORT_UNSPECIFIED,
		map[string]string{"ork.ns.team": "payments"})
	w := exp.Open(context.Background(), in, 1)
	if w == nil {
		t.Fatal("Open = nil, want a writer")
	}
	w.Write("hello")

	if len(bExporter.records) != 1 {
		t.Fatalf("sink b got %d records, want 1", len(bExporter.records))
	}
	if len(defExporter.records) != 0 {
		t.Fatalf("default sink got %d records, want 0", len(defExporter.records))
	}

	var gotAttr string
	var hasAttr bool
	bExporter.records[0].WalkAttributes(func(kv attribute.KeyValue) bool {
		if string(kv.Key) == "ork.ns.team" {
			gotAttr = kv.Value.AsString()
			hasAttr = true
		}
		return true
	})
	if !hasAttr || gotAttr != "payments" {
		t.Errorf("ork.ns.team = %q (present=%v), want %q", gotAttr, hasAttr, "payments")
	}
}

// TestTaskLogExporterForSinks_UnknownSinkFallsBackToDefault confirms a
// selection naming an undeclared sink still exports, to the default.
func TestTaskLogExporterForSinks_UnknownSinkFallsBackToDefault(t *testing.T) {
	defExporter := &recordingExporter{}
	defReader := sdkmetric.NewManualReader()
	def := newTestProviders(t, defExporter, defReader)
	sinks := newSinksForTest(def, nil)

	exp := NewTaskLogExporterForSinks(sinks, 0, true)
	in := testInputForSinkWithLogs("does-not-exist", orkstackv1.TaskLogExport_TASK_LOG_EXPORT_UNSPECIFIED, nil)
	w := exp.Open(context.Background(), in, 1)
	if w == nil {
		t.Fatal("Open = nil, want a writer routed to the default sink")
	}
	w.Write("hello")
	if len(defExporter.records) != 1 {
		t.Errorf("default sink got %d records, want 1", len(defExporter.records))
	}
}

// TestTaskLogExporterForSinks_SinkWithLogsDisabledStaysNil confirms a
// selection can pick a destination but cannot force logs on for a sink whose
// own Providers has logs switched off.
func TestTaskLogExporterForSinks_SinkWithLogsDisabledStaysNil(t *testing.T) {
	def := newTestProviders(t, &recordingExporter{}, sdkmetric.NewManualReader())
	noLogs := &Providers{
		Logs:  def.Logs,
		Meter: def.Meter,
		cfg:   Config{MetricsExporters: []string{ExporterOTLP}}, // logs off
	}
	sinks := newSinksForTest(def, map[string]*Providers{"quiet": noLogs})
	exp := NewTaskLogExporterForSinks(sinks, 0, true)

	in := testInputForSinkWithLogs("quiet", orkstackv1.TaskLogExport_TASK_LOG_EXPORT_ENABLED, nil)
	w := exp.Open(context.Background(), in, 1)
	if w != nil {
		t.Errorf("Open on a sink with logs disabled = %v, want nil", w)
	}
}

// blockingExporter blocks in Export until told to proceed, simulating a slow
// or unreachable collector.
type blockingExporter struct {
	release chan struct{}
	mu      sync.Mutex
	count   int
}

func (e *blockingExporter) Export(ctx context.Context, records []sdklog.Record) error {
	select {
	case <-e.release:
	case <-ctx.Done():
	}
	e.mu.Lock()
	e.count += len(records)
	e.mu.Unlock()
	return nil
}
func (e *blockingExporter) Shutdown(context.Context) error   { return nil }
func (e *blockingExporter) ForceFlush(context.Context) error { return nil }

func TestTaskLogWriterNeverBlocksUnderBackpressure(t *testing.T) {
	blocking := &blockingExporter{release: make(chan struct{})}

	provider := sdklog.NewLoggerProvider(sdklog.WithProcessor(
		sdklog.NewBatchProcessor(blocking, sdklog.WithMaxQueueSize(4), sdklog.WithExportMaxBatchSize(4)),
	))

	providers := &Providers{
		Logs:  provider,
		Meter: sdkmetric.NewMeterProvider(),
		cfg:   Config{LogsExporter: ExporterOTLP, MetricsExporters: nil},
	}
	exp := NewTaskLogExporter(providers, 0)
	w := exp.Open(context.Background(), testInput(), 1)

	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			w.Write(strings.Repeat("x", 8))
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		close(blocking.release)
		t.Fatal("Write blocked under a slow exporter and full queue")
	}

	// Unblock the exporter and tear down in an order that lets Shutdown's own
	// flush complete, rather than hang on an exporter that never returns.
	w.Close()
	close(blocking.release)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := provider.Shutdown(shutdownCtx); err != nil {
		t.Logf("provider.Shutdown: %v (acceptable under a bounded deadline)", err)
	}
}
