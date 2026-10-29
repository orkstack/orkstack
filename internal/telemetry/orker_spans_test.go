package telemetry

import (
	"context"
	"errors"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// newObserverProviders builds a Providers over an in-memory span exporter (a
// synchronous WithSyncer, so a span is visible the instant End/Phase returns
// — no batching to wait out) and a ManualReader for the metric side.
func newObserverProviders(t *testing.T) (*Providers, *tracetest.InMemoryExporter, *sdkmetric.ManualReader) {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	return &Providers{
		Tracer:     tp,
		Meter:      mp,
		Propagator: propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}),
		cfg:        Config{TracesExporter: ExporterOTLP, MetricsExporters: []string{ExporterOTLP}},
	}, exporter, reader
}

func observerTestInput() *orkstackv1.ExecuteTaskInput {
	return &orkstackv1.ExecuteTaskInput{
		RunId:          "r1",
		TaskId:         "t1",
		Namespace:      "ns1",
		OrkflowName:    "of1",
		OrkflowVersion: "v1",
		Pool:           "pool1",
	}
}

// attrString reads a string attribute off a span or a data point's attribute
// set, or "" if absent.
func attrString(attrs []attribute.KeyValue, key string) (string, bool) {
	for _, kv := range attrs {
		if string(kv.Key) == key {
			return kv.Value.AsString(), true
		}
	}
	return "", false
}

// TestOrkerObserver_PhaseSpansNestUnderTheAttempt is the trace half of the
// contract: Begin starts the attempt span, three Phase calls each end the
// previous phase span and start a new child, and End ends the last phase
// span and the attempt span — one attempt span, three phase spans, all
// children of the attempt.
func TestOrkerObserver_PhaseSpansNestUnderTheAttempt(t *testing.T) {
	providers, exporter, _ := newObserverProviders(t)
	inst, err := NewOrkerInstruments(providers)
	if err != nil {
		t.Fatalf("NewOrkerInstruments: %v", err)
	}
	obs := NewOrkerObserver(providers, inst, nil)

	in := observerTestInput()
	_, attemptObs := obs.Begin(context.Background(), in, 2)
	attemptObs.Phase("workspace")
	attemptObs.Phase("run")
	attemptObs.Phase("commit")
	attemptObs.End(orker.AttemptOutcome{})

	spans := exporter.GetSpans()
	var attempt *tracetest.SpanStub
	var phases []tracetest.SpanStub
	for i := range spans {
		s := spans[i]
		switch s.Name {
		case "ork.task.attempt":
			attempt = &spans[i]
		case "ork.task.phase":
			phases = append(phases, s)
		}
	}
	if attempt == nil {
		t.Fatal("no ork.task.attempt span was exported")
	}
	if len(phases) != 3 {
		t.Fatalf("got %d ork.task.phase spans, want 3 (workspace, run, commit)", len(phases))
	}
	wantPhase := []string{"workspace", "run", "commit"}
	for i, phase := range phases {
		if phase.Parent.SpanID() != attempt.SpanContext.SpanID() {
			t.Errorf("phase span %d's parent span id = %s, want the attempt span's %s",
				i, phase.Parent.SpanID(), attempt.SpanContext.SpanID())
		}
		got, _ := attrString(phase.Attributes, AttrPhase)
		if got != wantPhase[i] {
			t.Errorf("phase span %d ork.phase = %q, want %q", i, got, wantPhase[i])
		}
	}
	if got, _ := attrString(attempt.Attributes, AttrRunID); got != "r1" {
		t.Errorf("attempt span ork.run_id = %q, want %q", got, "r1")
	}
	// AttrAttempt is an int64 attribute, so it is read with AsInt64 below
	// rather than through the string-typed attrString helper.
	foundAttempt := false
	for _, kv := range attempt.Attributes {
		if string(kv.Key) == AttrAttempt && kv.Value.AsInt64() == 2 {
			foundAttempt = true
		}
	}
	if !foundAttempt {
		t.Errorf("attempt span attributes = %+v, want ork.attempt = 2", attempt.Attributes)
	}
}

// TestOrkerObserver_FailureSetsSpanStatusAndReason covers the failure path:
// End with a non-empty Reason sets the attempt span's status to Error and
// carries ork.failure.reason.
func TestOrkerObserver_FailureSetsSpanStatusAndReason(t *testing.T) {
	providers, exporter, _ := newObserverProviders(t)
	inst, err := NewOrkerInstruments(providers)
	if err != nil {
		t.Fatalf("NewOrkerInstruments: %v", err)
	}
	obs := NewOrkerObserver(providers, inst, nil)

	_, attemptObs := obs.Begin(context.Background(), observerTestInput(), 1)
	attemptObs.Phase("run")
	attemptObs.End(orker.AttemptOutcome{Reason: "CONTAINER_EXIT_NONZERO", ExitCode: 1})

	spans := exporter.GetSpans()
	var attempt *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "ork.task.attempt" {
			attempt = &spans[i]
		}
	}
	if attempt == nil {
		t.Fatal("no ork.task.attempt span was exported")
	}
	if attempt.Status.Code != codes.Error {
		t.Errorf("attempt span status = %v, want codes.Error", attempt.Status.Code)
	}
	if got, _ := attrString(attempt.Attributes, AttrFailureReason); got != "CONTAINER_EXIT_NONZERO" {
		t.Errorf("attempt span ork.failure.reason = %q, want %q", got, "CONTAINER_EXIT_NONZERO")
	}
	if got, _ := attrString(attempt.Attributes, AttrOutcome); got != OutcomeFailed {
		t.Errorf("attempt span outcome = %q, want %q", got, OutcomeFailed)
	}
}

// TestOrkerObserver_PhaseHistogramHasOnePointPerEndedPhase is the metric half:
// three Phase calls followed by End must leave exactly three recorded points
// on ork.orker.task_phase.duration (one per ended phase — "run" ends when
// Phase("commit") is called, and "commit" ends at End).
func TestOrkerObserver_PhaseHistogramHasOnePointPerEndedPhase(t *testing.T) {
	providers, _, reader := newObserverProviders(t)
	inst, err := NewOrkerInstruments(providers)
	if err != nil {
		t.Fatalf("NewOrkerInstruments: %v", err)
	}
	obs := NewOrkerObserver(providers, inst, nil)

	_, attemptObs := obs.Begin(context.Background(), observerTestInput(), 1)
	attemptObs.Phase("workspace")
	attemptObs.Phase("run")
	attemptObs.Phase("commit")
	attemptObs.End(orker.AttemptOutcome{})

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	points := histogramPoints(t, rm, "ork.orker.task_phase.duration")
	if len(points) != 3 {
		t.Fatalf("got %d data points on ork.orker.task_phase.duration, want 3", len(points))
	}
	seen := map[string]bool{}
	for _, dp := range points {
		if v, ok := dp.Attributes.Value(attribute.Key(AttrPhase)); ok {
			seen[v.AsString()] = true
		}
	}
	for _, phase := range []string{"workspace", "run", "commit"} {
		if !seen[phase] {
			t.Errorf("no ork.orker.task_phase.duration point carries phase=%q; saw %v", phase, seen)
		}
	}
}

// histogramPoints returns the Float64 histogram data points recorded for
// name, or nil if the metric was never recorded.
func histogramPoints(t *testing.T, rm metricdata.ResourceMetrics, name string) []metricdata.HistogramDataPoint[float64] {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				continue
			}
			return hist.DataPoints
		}
	}
	return nil
}

// TestOrkerObserver_TraceContext covers both halves of the seam: the ctx
// Begin returns yields a real traceparent, and an unrelated context (no
// span) yields an empty map — never nil, never a key with an empty value.
func TestOrkerObserver_TraceContext(t *testing.T) {
	providers, _, _ := newObserverProviders(t)
	inst, err := NewOrkerInstruments(providers)
	if err != nil {
		t.Fatalf("NewOrkerInstruments: %v", err)
	}
	obs := NewOrkerObserver(providers, inst, nil)

	ctx, attemptObs := obs.Begin(context.Background(), observerTestInput(), 1)
	defer attemptObs.End(orker.AttemptOutcome{})

	carrier := obs.TraceContext(ctx)
	if carrier["traceparent"] == "" {
		t.Errorf("TraceContext(ctx from Begin) = %v, want a non-empty traceparent", carrier)
	}

	empty := obs.TraceContext(context.Background())
	if len(empty) != 0 {
		t.Errorf("TraceContext(context.Background()) = %v, want an empty map (no valid span)", empty)
	}
}

// TestOrkerObserver_CardinalityGatesRunAndTaskIDOnTasksCounter is the same
// high-vs-low-cardinality contract Cardinality.TaskAttrs already documents,
// exercised through the observer's own instrument: ork.orker.tasks carries
// ork.run_id and ork.task_id only when HighCardinality is set.
func TestOrkerObserver_CardinalityGatesRunAndTaskIDOnTasksCounter(t *testing.T) {
	for _, high := range []bool{false, true} {
		high := high
		t.Run(map[bool]string{false: "low", true: "high"}[high], func(t *testing.T) {
			providers, _, reader := newObserverProviders(t)
			providers.cfg.HighCardinality = high
			inst, err := NewOrkerInstruments(providers)
			if err != nil {
				t.Fatalf("NewOrkerInstruments: %v", err)
			}
			obs := NewOrkerObserver(providers, inst, nil)

			_, attemptObs := obs.Begin(context.Background(), observerTestInput(), 1)
			attemptObs.End(orker.AttemptOutcome{})

			var rm metricdata.ResourceMetrics
			if err := reader.Collect(context.Background(), &rm); err != nil {
				t.Fatalf("Collect: %v", err)
			}
			dp := onlySumPoint(t, rm, "ork.orker.tasks")
			_, hasRunID := dp.Attributes.Value(attribute.Key(AttrRunID))
			if hasRunID != high {
				t.Errorf("ork.orker.tasks carries ork.run_id = %v, want %v (HighCardinality=%v)",
					hasRunID, high, high)
			}
		})
	}
}

// inputForSink returns an ExecuteTaskInput pinned to sink, carrying attrs as
// its telemetry selection's attributes.
func inputForSink(sink string, attrs map[string]string) *orkstackv1.ExecuteTaskInput {
	in := observerTestInput()
	in.Telemetry = &orkstackv1.TelemetrySelection{Sink: sink, Attributes: attrs}
	return in
}

// TestNewOrkerObserverForSinks_TaskPinnedToNamedSinkLandsThere is the sink
// routing contract: an attempt pinned to sink "b" produces its span on b's
// in-memory exporter, not the default sink's, and carries the selection's
// attributes.
func TestNewOrkerObserverForSinks_TaskPinnedToNamedSinkLandsThere(t *testing.T) {
	defProviders, defExporter, _ := newObserverProviders(t)
	bProviders, bExporter, _ := newObserverProviders(t)

	sinks := newSinksForTest(defProviders, map[string]*Providers{"b": bProviders})
	obs := NewOrkerObserverForSinks(sinks, nil)

	in := inputForSink("b", map[string]string{"ork.ns.team": "payments"})
	_, attemptObs := obs.Begin(context.Background(), in, 1)
	attemptObs.End(orker.AttemptOutcome{})

	if len(defExporter.GetSpans()) != 0 {
		t.Fatalf("default sink got %d spans, want 0", len(defExporter.GetSpans()))
	}
	spans := bExporter.GetSpans()
	var attempt *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "ork.task.attempt" {
			attempt = &spans[i]
		}
	}
	if attempt == nil {
		t.Fatal("sink b got no ork.task.attempt span")
	}
	if got, ok := attrString(attempt.Attributes, "ork.ns.team"); !ok || got != "payments" {
		t.Errorf("attempt span ork.ns.team = %q (present=%v), want %q", got, ok, "payments")
	}
}

// TestNewOrkerObserverForSinks_UnknownSinkFallsBackToDefault confirms an
// attempt naming an undeclared sink still traces, on the default.
func TestNewOrkerObserverForSinks_UnknownSinkFallsBackToDefault(t *testing.T) {
	defProviders, defExporter, _ := newObserverProviders(t)
	sinks := newSinksForTest(defProviders, nil)
	obs := NewOrkerObserverForSinks(sinks, nil)

	in := inputForSink("does-not-exist", nil)
	_, attemptObs := obs.Begin(context.Background(), in, 1)
	attemptObs.End(orker.AttemptOutcome{})

	found := false
	for _, s := range defExporter.GetSpans() {
		if s.Name == "ork.task.attempt" {
			found = true
		}
	}
	if !found {
		t.Error("default sink got no ork.task.attempt span for an unknown sink name")
	}
}

// TestNewOrkerObserverForSinks_SelectionAttrsOnMetricsOnlyHighCardinality
// covers the metric half of the trade-off: the selection's attributes always
// land on the span (already covered above), but only land on
// ork.orker.tasks when that sink's Cardinality is High.
func TestNewOrkerObserverForSinks_SelectionAttrsOnMetricsOnlyHighCardinality(t *testing.T) {
	for _, high := range []bool{false, true} {
		high := high
		t.Run(map[bool]string{false: "low", true: "high"}[high], func(t *testing.T) {
			providers, _, reader := newObserverProviders(t)
			providers.cfg.HighCardinality = high
			sinks := newSinksForTest(providers, nil)
			obs := NewOrkerObserverForSinks(sinks, nil)

			in := inputForSink("", map[string]string{"ork.ns.team": "payments"})
			_, attemptObs := obs.Begin(context.Background(), in, 1)
			attemptObs.End(orker.AttemptOutcome{})

			var rm metricdata.ResourceMetrics
			if err := reader.Collect(context.Background(), &rm); err != nil {
				t.Fatalf("Collect: %v", err)
			}
			dp := onlySumPoint(t, rm, "ork.orker.tasks")
			_, hasAttr := dp.Attributes.Value(attribute.Key("ork.ns.team"))
			if hasAttr != high {
				t.Errorf("ork.orker.tasks carries ork.ns.team = %v, want %v (HighCardinality=%v)", hasAttr, high, high)
			}
		})
	}
}

// TestOrkerObserver_TraceContextUsesDefaultSinkPropagator confirms
// TraceContext always uses the default sink's route, even when the attempt
// that produced ctx was pinned to a named sink.
func TestOrkerObserver_TraceContextUsesDefaultSinkPropagator(t *testing.T) {
	defProviders, _, _ := newObserverProviders(t)
	bProviders, _, _ := newObserverProviders(t)
	sinks := newSinksForTest(defProviders, map[string]*Providers{"b": bProviders})
	obs := NewOrkerObserverForSinks(sinks, nil)

	in := inputForSink("b", nil)
	ctx, attemptObs := obs.Begin(context.Background(), in, 1)
	defer attemptObs.End(orker.AttemptOutcome{})

	carrier := obs.TraceContext(ctx)
	if carrier["traceparent"] == "" {
		t.Errorf("TraceContext(ctx from an attempt pinned to sink b) = %v, want a non-empty traceparent", carrier)
	}
}

// onlySumPoint returns the single int64 sum data point recorded for name.
func onlySumPoint(t *testing.T, rm metricdata.ResourceMetrics, name string) metricdata.DataPoint[int64] {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok || len(sum.DataPoints) == 0 {
				continue
			}
			return sum.DataPoints[0]
		}
	}
	t.Fatalf("no data point recorded for %s", name)
	return metricdata.DataPoint[int64]{}
}

// TestOrkerObserver_CountsAnAttemptWhoseDirectoryWasLeftOnDisk: an attempt
// whose own directory, or its secrets, could not be removed is counted on
// ork.orker.workspace_cleanup_failures, so a disk filling with leftovers is a
// number an operator can alert on, not a log line to go looking for. An
// attempt that cleaned up after itself records no point.
func TestOrkerObserver_CountsAnAttemptWhoseDirectoryWasLeftOnDisk(t *testing.T) {
	providers, _, reader := newObserverProviders(t)
	obs := NewOrkerObserverForSinks(newSinksForTest(providers, nil), nil)

	_, clean := obs.Begin(context.Background(), inputForSink("", nil), 1)
	clean.End(orker.AttemptOutcome{})
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "ork.orker.workspace_cleanup_failures" {
				t.Fatalf("an attempt that removed its directory recorded %s", m.Name)
			}
		}
	}

	_, leaked := obs.Begin(context.Background(), inputForSink("", nil), 1)
	leaked.End(orker.AttemptOutcome{Reason: orker.ReasonContainerExitNonzero, CleanupErr: errors.New("permission denied")})
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if dp := onlySumPoint(t, rm, "ork.orker.workspace_cleanup_failures"); dp.Value != 1 {
		t.Fatalf("ork.orker.workspace_cleanup_failures = %d; want 1", dp.Value)
	}
}
