package telemetry

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// droppedRecords reads ork.telemetry.records_dropped by "signal/reason".
func droppedRecords(t *testing.T, reader sdkmetric.Reader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	must.OK(t, reader.Collect(context.Background(), &rm))
	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != RecordsDroppedMetric {
				continue
			}
			for _, p := range m.Data.(metricdata.Sum[int64]).DataPoints {
				signal, _ := p.Attributes.Value(AttrSignal)
				reason, _ := p.Attributes.Value(AttrDropReason)
				got[signal.AsString()+"/"+reason.AsString()] += p.Value
			}
		}
	}
	return got
}

func hasMetric(t *testing.T, reader sdkmetric.Reader, name string) bool {
	t.Helper()
	var rm metricdata.ResourceMetrics
	must.OK(t, reader.Collect(context.Background(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return true
			}
		}
	}
	return false
}

// TestSinks_ForNamespacedRefusesUnknown: "" and the default sink name are the
// default, a declared sink is itself, and an undeclared one is refused, with
// one Warn however often it is asked for.
func TestSinks_ForNamespacedRefusesUnknown(t *testing.T) {
	def := &Providers{}
	team := &Providers{}
	s := newSinksForTest(def, map[string]*Providers{"team": team})
	var buf bytes.Buffer
	s.WithLogger(slog.New(slog.NewTextHandler(&buf, nil)))
	for _, tc := range []struct {
		name string
		want *Providers
		ok   bool
	}{
		{"", def, true},
		{DefaultSink, def, true},
		{"team", team, true},
		{"ghost", nil, false},
		{"ghost", nil, false},
	} {
		got, ok := s.ForNamespaced(tc.name)
		if got != tc.want || ok != tc.ok {
			t.Fatalf("ForNamespaced(%q) = (%p, %v); want (%p, %v)", tc.name, got, ok, tc.want, tc.ok)
		}
	}
	if n := strings.Count(buf.String(), "level=WARN"); n != 1 || !strings.Contains(buf.String(), "sink=ghost") {
		t.Fatalf("want one Warn naming the sink:\n%s", buf.String())
	}
}

// TestRunBoundRecords_NeverFallBackToDefault: a run, task log or attempt
// naming a sink this process does not declare sends nothing to the default
// sink; each record is dropped and counted there, by signal, as
// sink_unknown (OPEN-22).
func TestRunBoundRecords_NeverFallBackToDefault(t *testing.T) {
	t.Run("run metrics", func(t *testing.T) {
		reader := sdkmetric.NewManualReader()
		def := newTestProviders(t, &recordingExporter{}, reader)
		observer, err := NewEngineObserverForSinks(newSinksForTest(def, nil), slog.New(slog.DiscardHandler))
		must.OK(t, err)
		started := time.Unix(1755302400, 0).UTC()
		run := &orkstackv1.Run{
			Id: "run-1", Namespace: "team", OrkflowName: "hello", Status: orkstackv1.Status_STATUS_SUCCEEDED,
			StartedAt: timestamppb.New(started), FinishedAt: timestamppb.New(started.Add(time.Second)),
			Telemetry: &orkstackv1.TelemetrySelection{Sink: "ghost"},
		}
		observer.ObserveEvent(context.Background(),
			runEvent("run-1", orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, orkstackv1.Status_STATUS_SUCCEEDED, ""), run)
		observer.ObserveRefusal(context.Background(), "team", &orkstackv1.TelemetrySelection{Sink: "ghost"}, "hello", "NAMESPACE_HELD")
		if hasMetric(t, reader, "ork.runs") || hasMetric(t, reader, "ork.runs.refused") {
			t.Fatal("the default sink recorded a run naming an undeclared sink")
		}
		if got := droppedRecords(t, reader)[SignalMetrics+"/"+DropReasonSinkUnknown]; got != 2 {
			t.Fatalf("dropped metrics records = %d; want 2", got)
		}
	})

	t.Run("task logs", func(t *testing.T) {
		exporter := &recordingExporter{}
		reader := sdkmetric.NewManualReader()
		def := newTestProviders(t, exporter, reader)
		exp := NewTaskLogExporterForSinks(newSinksForTest(def, nil), 0, true)
		w := exp.Open(context.Background(),
			testInputForSinkWithLogs("ghost", orkstackv1.TaskLogExport_TASK_LOG_EXPORT_UNSPECIFIED, nil), 1)
		w.Write("one")
		w.Write("two")
		w.Close()
		w.Write("after close")
		if len(exporter.records) != 0 {
			t.Fatalf("the default sink got %d task log records; want none", len(exporter.records))
		}
		if got := droppedRecords(t, reader)[SignalLogs+"/"+DropReasonSinkUnknown]; got != 2 {
			t.Fatalf("dropped log records = %d; want 2", got)
		}
	})

	t.Run("attempt span and metrics", func(t *testing.T) {
		def, spans, reader := newObserverProviders(t)
		obs := NewOrkerObserverForSinks(newSinksForTest(def, nil), nil)
		_, attempt := obs.Begin(context.Background(), inputForSink("ghost", nil), 1)
		attempt.Phase("pull")
		attempt.End(orker.AttemptOutcome{})
		if n := len(spans.GetSpans()); n != 0 {
			t.Fatalf("the default sink got %d spans; want none", n)
		}
		if hasMetric(t, reader, "ork.orker.tasks") {
			t.Fatal("the default sink recorded the attempt's metrics")
		}
		dropped := droppedRecords(t, reader)
		if dropped[SignalTraces+"/"+DropReasonSinkUnknown] != 1 || dropped[SignalMetrics+"/"+DropReasonSinkUnknown] != 1 {
			t.Fatalf("dropped = %v; want one attempt's traces and metrics", dropped)
		}
	})
}

// TestSinks_AnyMetrics: censuses start when any sink, default or named,
// exports metrics.
func TestSinks_AnyMetrics(t *testing.T) {
	on := Config{MetricsExporters: []string{ExporterOTLP}}
	for _, tc := range []struct {
		name string
		s    *Sinks
		want bool
	}{
		{"nil", nil, false},
		{"none", newSinksForTest(&Providers{}, map[string]*Providers{"team": {}}), false},
		{"default", newSinksForTest(&Providers{cfg: on}, nil), true},
		{"named only", newSinksForTest(&Providers{}, map[string]*Providers{"team": {cfg: on}}), true},
	} {
		if got := tc.s.AnyMetrics(); got != tc.want {
			t.Errorf("%s: AnyMetrics = %v; want %v", tc.name, got, tc.want)
		}
	}
}

// TestSinks_DropsAreLoggedWhenNoMetricsCountThem: with the default sink
// exporting no metrics, the first drop of each signal and reason is logged,
// so a drop is never silent.
func TestSinks_DropsAreLoggedWhenNoMetricsCountThem(t *testing.T) {
	var buf bytes.Buffer
	s := newSinksForTest(&Providers{}, nil)
	s.WithLogger(slog.New(slog.NewTextHandler(&buf, nil)))
	for range 3 {
		s.DropRecords(context.Background(), SignalMetrics, DropReasonUnrouted, 1)
	}
	s.DropRecords(context.Background(), SignalLogs, DropReasonUnrouted, 1)
	if n := strings.Count(buf.String(), "level=WARN"); n != 2 {
		t.Fatalf("log = %q; want one Warn per signal and reason", buf.String())
	}
}
