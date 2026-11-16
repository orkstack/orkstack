package telemetry

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// exportErrorCounts reads ork.telemetry.export_errors by signal.
func exportErrorCounts(t *testing.T, reader sdkmetric.Reader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	must.OK(t, reader.Collect(context.Background(), &rm))
	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != ExportErrorsMetric {
				continue
			}
			for _, p := range m.Data.(metricdata.Sum[int64]).DataPoints {
				v, _ := p.Attributes.Value(AttrSignal)
				got[v.String()] += p.Value
			}
		}
	}
	return got
}

// failingSpanExporter refuses every batch.
type failingSpanExporter struct{ tracetest.InMemoryExporter }

func (*failingSpanExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error {
	return errors.New("collector refused the batch")
}

// TestErrorHandler_CountsExportFailures: every error the SDK reports is
// counted once, by the signal whose exporter failed, and logged at most once
// per signal per interval with how many were suppressed.
func TestErrorHandler_CountsExportFailures(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	h, err := newExportErrorHandler(provider)
	must.OK(t, err)
	now := time.Unix(1000, 0)
	h.now = func() time.Time { return now }
	var buf bytes.Buffer
	h.setLogger(slog.New(slog.NewTextHandler(&buf, nil)))

	// The exporter wrappers say which signal failed, through any wrapping
	// the SDK adds.
	traces := signalSpanExporter{SpanExporter: &failingSpanExporter{}}
	tagged := traces.ExportSpans(context.Background(), nil)
	h.Handle(tagged)
	h.Handle(errors.Join(errors.New("sdk context"), tagSignal("", SignalMetrics, errors.New("503"))))
	h.Handle(tagSignal("", SignalLogs, errors.New("timeout")))
	h.Handle(tagSignal("", SignalLogs, errors.New("timeout")))
	h.Handle(errors.New("an error no exporter tagged"))
	h.Handle(nil)

	want := map[string]int64{SignalTraces: 1, SignalMetrics: 1, SignalLogs: 2, SignalOther: 1}
	got := exportErrorCounts(t, reader)
	for signal, n := range want {
		if got[signal] != n {
			t.Errorf("%s{%s} = %d; want %d (all: %v)", ExportErrorsMetric, signal, got[signal], n, got)
		}
	}
	if n := strings.Count(buf.String(), "telemetry export failed"); n != 3 {
		t.Fatalf("export lines = %d; want one per failed signal (3):\n%s", n, buf.String())
	}
	if n := strings.Count(buf.String(), "telemetry error"); n != 1 {
		t.Fatalf("other lines = %d; want 1 for the error no exporter tagged:\n%s", n, buf.String())
	}
	now = now.Add(exportErrorLogInterval)
	h.Handle(tagSignal("", SignalLogs, errors.New("timeout")))
	if !strings.Contains(buf.String(), "suppressed=1") {
		t.Fatalf("the next line after the interval does not count the suppressed one:\n%s", buf.String())
	}
	if !errors.Is(tagged, tagged) || tagged.Error() != "collector refused the batch" {
		t.Fatalf("a tagged error reads %q; want the exporter's own text", tagged)
	}
}

// TestErrorHandler_LogsNoDestinationCredential: an export error's URL loses
// its user, password, query and fragment before it is logged; the host and
// path stay.
func TestErrorHandler_LogsNoDestinationCredential(t *testing.T) {
	provider := sdkmetric.NewMeterProvider()
	h, err := newExportErrorHandler(provider)
	must.OK(t, err)
	var buf bytes.Buffer
	h.setLogger(slog.New(slog.NewTextHandler(&buf, nil)))
	h.Handle(tagSignal("", SignalTraces, errors.New(
		`Post "https://user:pa55@collector.example/v1/traces?api_key=s3cret#frag": dial tcp: connection refused`)))
	got := buf.String()
	for _, secret := range []string{"pa55", "user:", "s3cret", "api_key", "frag"} {
		if strings.Contains(got, secret) {
			t.Fatalf("log holds %q:\n%s", secret, got)
		}
	}
	if !strings.Contains(got, "https://collector.example/v1/traces") || !strings.Contains(got, "connection refused") {
		t.Fatalf("log lost the destination or the cause:\n%s", got)
	}
}

// TestErrorHandler_OneSinkDownHidesNoOtherSink: each sink's failures are
// rate-limited on their own and logged under the sink's name, and a nil
// logger keeps the handler logging.
func TestErrorHandler_OneSinkDownHidesNoOtherSink(t *testing.T) {
	h, err := newExportErrorHandler(sdkmetric.NewMeterProvider())
	must.OK(t, err)
	var buf bytes.Buffer
	h.setLogger(slog.New(slog.NewTextHandler(&buf, nil)))
	h.setLogger(nil)
	h.Handle(tagSignal("team-a", SignalTraces, errors.New("connection refused")))
	h.Handle(tagSignal("team-a", SignalTraces, errors.New("connection refused")))
	h.Handle(tagSignal("team-b", SignalTraces, errors.New("x509: certificate signed by unknown authority")))
	got := buf.String()
	if n := strings.Count(got, "telemetry export failed"); n != 2 {
		t.Fatalf("lines = %d; want one per sink (2):\n%s", n, got)
	}
	for _, want := range []string{"sink=team-a", "sink=team-b", "x509"} {
		if !strings.Contains(got, want) {
			t.Fatalf("log lacks %q:\n%s", want, got)
		}
	}
}

// TestErrorHandler_RedactsAURLErrorsOwnURL: a *url.Error's URL loses its
// user and password even when the password holds an unescaped @.
func TestErrorHandler_RedactsAURLErrorsOwnURL(t *testing.T) {
	raw := "https://user:p@ss@collector.example/v1/logs?token=s3cret"
	text := exportErrorText(tagSignal("", SignalLogs, &url.Error{Op: "Post", URL: raw, Err: errors.New("connection refused")}))
	for _, secret := range []string{"p@ss", "ss@", "s3cret", "user"} {
		if strings.Contains(text, secret) {
			t.Fatalf("%q holds %q", text, secret)
		}
	}
	if !strings.Contains(text, "collector.example/v1/logs") {
		t.Fatalf("%q lost the destination", text)
	}
}

// TestDeadCollector_FailsNothing: with every signal pointed at a collector
// that is not there, recording spans, metrics and log records returns at
// once, the failures are counted on the export-errors counter, and Shutdown
// returns within its bound. Telemetry can never fail a task (AGENTS.md §11).
func TestDeadCollector_FailsNothing(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must.OK(t, err)
	dead := "http://" + listener.Addr().String()
	must.OK(t, listener.Close())
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", dead)
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "200")
	t.Setenv("OTEL_BSP_SCHEDULE_DELAY", "10")
	t.Setenv("OTEL_BLRP_SCHEDULE_DELAY", "10")
	t.Setenv("OTEL_METRIC_EXPORT_INTERVAL", "20")
	previous := otel.GetErrorHandler()
	t.Cleanup(func() { otel.SetErrorHandler(previous) })

	cfg := Config{
		ServiceName:      ServiceOrker,
		Component:        ComponentOrker,
		LogsExporter:     ExporterOTLP,
		TracesExporter:   ExporterOTLP,
		MetricsExporters: []string{ExporterOTLP, ExporterPrometheus},
	}
	sinks, err := SetupSinks(context.Background(), cfg)
	must.OK(t, err)
	p := sinks.Default()

	ctx := context.Background()
	_, span := p.Tracer.Tracer(ScopeName).Start(ctx, "task")
	span.End()
	counter, err := p.Meter.Meter(ScopeName).Int64Counter("ork.test.recorded")
	must.OK(t, err)
	counter.Add(ctx, 1, metric.WithAttributes(attribute.String("k", "v")))
	var record otellog.Record
	record.SetBody(attribute.StringValue("a task log line"))
	p.Logs.Logger(ScopeName).Emit(ctx, record)

	deadline := time.Now().Add(30 * time.Second)
	for {
		families, err := p.Registry.Gather()
		must.OK(t, err)
		seen := map[string]bool{}
		for _, f := range families {
			if f.GetName() != "ork_telemetry_export_errors_total" {
				continue
			}
			for _, m := range f.GetMetric() {
				for _, l := range m.GetLabel() {
					if l.GetName() == "ork_telemetry_signal" && m.GetCounter().GetValue() > 0 {
						seen[l.GetValue()] = true
					}
				}
			}
		}
		if seen[SignalTraces] && seen[SignalMetrics] && seen[SignalLogs] {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("export failures counted for %v; want traces, metrics and logs", seen)
		}
		time.Sleep(20 * time.Millisecond)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		_ = sinks.Shutdown(shutdownCtx) // the dead collector's error is expected
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Shutdown did not return within its bound")
	}
}

// TestDeadCollector_TaskAttemptFailsNothing: a whole task attempt's
// telemetry — its spans and phases, its task log lines and its metrics —
// against a collector that is not there returns at once at every step, so an
// attempt never waits on, or fails because of, its telemetry (AGENTS.md §11).
func TestDeadCollector_TaskAttemptFailsNothing(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must.OK(t, err)
	dead := "http://" + listener.Addr().String()
	must.OK(t, listener.Close())
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", dead)
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "200")
	previous := otel.GetErrorHandler()
	t.Cleanup(func() { otel.SetErrorHandler(previous) })
	sinks, err := SetupSinks(context.Background(), Config{
		ServiceName: ServiceOrker, Component: ComponentOrker,
		LogsExporter: ExporterOTLP, TracesExporter: ExporterOTLP, MetricsExporters: []string{ExporterOTLP},
	})
	must.OK(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = sinks.Shutdown(ctx) // the dead collector's error is expected
	})
	obs := NewOrkerObserverForSinks(sinks, nil)
	logs := NewTaskLogExporterForSinks(sinks, 0, true)

	start := time.Now()
	for attempt := range int32(20) {
		ctx, a := obs.Begin(context.Background(), observerTestInput(), attempt+1)
		w := logs.Open(ctx, observerTestInput(), attempt+1)
		for _, phase := range []string{"pull", "run", "commit"} {
			a.Phase(phase)
			for range 50 {
				w.Write("a line of the task's output")
			}
		}
		w.Close()
		a.End(orker.AttemptOutcome{})
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("20 attempts' telemetry against a dead collector took %v; recording must never wait on export", took)
	}
}
