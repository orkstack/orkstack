package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

func TestSetupZeroConfig(t *testing.T) {
	// Force a GC so leftover goroutines from earlier tests in the package
	// settle before the baseline count, or this becomes a flaky assertion.
	runtime.GC()
	before := runtime.NumGoroutine()

	p, err := Setup(context.Background(), Config{})
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if p.Enabled() {
		t.Errorf("Enabled() = true for zero Config")
	}
	if p.Registry != nil {
		t.Errorf("Registry = %v, want nil", p.Registry)
	}
	if h := TemporalMetricsHandler(p, nil); h != nil {
		t.Errorf("TemporalMetricsHandler = %v, want nil", h)
	}
	interceptors, err := TemporalInterceptors(p)
	if interceptors != nil || err != nil {
		t.Errorf("TemporalInterceptors = (%v, %v), want (nil, nil)", interceptors, err)
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown: %v", err)
	}

	// Give any accidentally-started background goroutine a moment to show up
	// before comparing counts.
	time.Sleep(50 * time.Millisecond)
	after := runtime.NumGoroutine()
	if after > before {
		t.Errorf("goroutines grew from %d to %d on zero Config Setup", before, after)
	}
}

func TestSetupPrometheus(t *testing.T) {
	cfg := Config{MetricsExporters: []string{ExporterPrometheus}, AdminAddr: "127.0.0.1:0"}
	p, err := Setup(context.Background(), cfg, attribute.String("service.version", "test"))
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	defer p.Shutdown(context.Background())

	if p.Registry == nil {
		t.Fatal("Registry is nil, want a Prometheus registry")
	}

	mux := NewAdminMux(p, cfg)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + MetricsPath)
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	body := make([]byte, 1<<20)
	n, _ := resp.Body.Read(body)
	text := string(body[:n])
	for _, want := range []string{"ork_build_info", "go_goroutines"} {
		if !strings.Contains(text, want) {
			t.Errorf("/metrics body missing %q", want)
		}
	}
}

func TestSetupOTLPExporterConstructs(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	cfg := Config{
		LogsExporter:     ExporterOTLP,
		MetricsExporters: []string{ExporterOTLP},
		TracesExporter:   ExporterOTLP,
	}
	p, err := Setup(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if !p.Enabled() {
		t.Errorf("Enabled() = false, want true")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Shutdown(ctx) }()
	select {
	case <-done:
		// An error here is fine — the collector at 127.0.0.1:1 refuses the
		// connection — the point is that Shutdown returns instead of hanging.
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown did not return within its budget")
	}
}

func TestSetupSinksZeroConfig(t *testing.T) {
	sinks, err := SetupSinks(context.Background(), Config{})
	if err != nil {
		t.Fatalf("SetupSinks: %v", err)
	}
	if sinks.Default() == nil {
		t.Fatal("Default() = nil")
	}
	if sinks.Default().Enabled() {
		t.Errorf("Default().Enabled() = true for zero Config")
	}
	if sinks.For("anything") != sinks.Default() {
		t.Errorf("For(anything) != Default() on zero Config")
	}
	if err := sinks.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
}

func TestSetupSinksNamed(t *testing.T) {
	table, err := ParseSinks([]byte(`
sinks:
  default:
    endpoint: http://127.0.0.1:1
  other:
    logs_endpoint: http://127.0.0.1:2
    headers:
      X-Test: { value: "literal" }
`))
	if err != nil {
		t.Fatalf("ParseSinks: %v", err)
	}
	if err := table.Resolve(os.LookupEnv, os.ReadFile); err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	cfg := Config{
		LogsExporter:     ExporterOTLP,
		MetricsExporters: []string{ExporterPrometheus},
		AdminAddr:        "127.0.0.1:0",
		Sinks:            table,
	}
	sinks, err := SetupSinks(context.Background(), cfg)
	if err != nil {
		t.Fatalf("SetupSinks: %v", err)
	}

	if got, want := sinks.Names(), []string{"default", "other"}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("Names() = %v, want %v", got, want)
	}
	if sinks.For("other") == sinks.Default() {
		t.Error("For(other) == Default(), want a distinct Providers")
	}
	if sinks.For("missing") != sinks.Default() {
		t.Error("For(missing) != Default(), want it to fall back")
	}

	// Prometheus lives only on the default sink, and For("missing") above
	// must have counted an unknown-sink lookup on it: scrape /metrics and
	// look for both.
	mux := NewAdminMux(sinks.Default(), cfg)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	resp, err := http.Get(srv.URL + MetricsPath)
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	body := make([]byte, 1<<20)
	n, _ := resp.Body.Read(body)
	text := string(body[:n])
	if !strings.Contains(text, "ork_telemetry_sink_unknown_total") {
		t.Errorf("/metrics missing ork_telemetry_sink_unknown_total:\n%s", text)
	}
	if !strings.Contains(text, `sink="missing"`) {
		t.Errorf("/metrics missing the sink=\"missing\" label:\n%s", text)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- sinks.Shutdown(ctx) }()
	select {
	case <-done:
		// An error is fine (127.0.0.1:1 and :2 refuse the connection); the
		// point is that Shutdown returns instead of hanging on either sink.
	case <-time.After(3 * time.Second):
		t.Fatal("Sinks.Shutdown did not return within its budget")
	}
}

// TestSetupSinksNamedSinkBuildsWithNoGlobalExporters is fix 1's contract: a
// named sink is built purely from cfg.Sinks, never gated on the global
// OTEL_*_EXPORTER flags — an operator who names a "datadog" sink with a
// logs_endpoint but never sets OTEL_LOGS_EXPORTER (or any other OTEL_*
// exporter) still gets a working, distinct logs destination for it.
func TestSetupSinksNamedSinkBuildsWithNoGlobalExporters(t *testing.T) {
	table, err := ParseSinks([]byte(`
sinks:
  datadog:
    logs_endpoint: http://127.0.0.1:1
`))
	if err != nil {
		t.Fatalf("ParseSinks: %v", err)
	}
	if err := table.Resolve(os.LookupEnv, os.ReadFile); err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	// No OTEL_*_EXPORTER anywhere in cfg — the zero-config path, plus a sink.
	cfg := Config{Sinks: table}
	sinks, err := SetupSinks(context.Background(), cfg)
	if err != nil {
		t.Fatalf("SetupSinks: %v", err)
	}

	if got, want := sinks.Names(), []string{"datadog", "default"}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("Names() = %v, want %v", got, want)
	}
	datadog := sinks.For("datadog")
	if datadog == sinks.Default() {
		t.Fatal("For(datadog) == Default(), want a distinct Providers")
	}
	if !datadog.Config().LogsEnabled() {
		t.Error("For(datadog).Config().LogsEnabled() = false, want true (spec names a logs_endpoint)")
	}
	if datadog.Config().TracesEnabled() {
		t.Error("For(datadog).Config().TracesEnabled() = true, want false (spec names no traces_endpoint)")
	}
	if sinks.Default().Enabled() {
		t.Error("Default().Enabled() = true, want false: no global exporter and no \"default\" sink entry")
	}
}

// TestSetupSinksMetricsOnlySinkBootsWithGlobalLogsOn is fix 2's contract: a
// named sink that names only a metrics_endpoint must not fail to boot just
// because the deployment's global OTEL_LOGS_EXPORTER is on — that signal is
// simply off for THIS sink, not a boot error.
func TestSetupSinksMetricsOnlySinkBootsWithGlobalLogsOn(t *testing.T) {
	table, err := ParseSinks([]byte(`
sinks:
  metrics-only:
    metrics_endpoint: http://127.0.0.1:1
`))
	if err != nil {
		t.Fatalf("ParseSinks: %v", err)
	}
	if err := table.Resolve(os.LookupEnv, os.ReadFile); err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	cfg := Config{LogsExporter: ExporterOTLP, Sinks: table}
	sinks, err := SetupSinks(context.Background(), cfg)
	if err != nil {
		t.Fatalf("SetupSinks: %v", err)
	}

	p := sinks.For("metrics-only")
	if !p.Config().MetricsEnabled() {
		t.Error("Config().MetricsEnabled() = false, want true")
	}
	if p.Config().LogsEnabled() {
		t.Error("Config().LogsEnabled() = true, want false: this sink names no logs_endpoint")
	}

	status := sinks.Describe()
	var found bool
	for _, s := range status {
		if s.Name != "metrics-only" {
			continue
		}
		found = true
		if !s.Metrics || s.Logs || s.Traces {
			t.Errorf("Describe()[metrics-only] = %+v, want {Metrics:true Logs:false Traces:false}", s)
		}
	}
	if !found {
		t.Error("Describe() has no entry for metrics-only")
	}
}

// TestSetupSinksUselessSinkRefusesBoot confirms the one boot error fix 2
// keeps: a sink with no endpoint anywhere is nonsensical, and is refused
// rather than silently built with every signal off.
func TestSetupSinksUselessSinkRefusesBoot(t *testing.T) {
	table, err := ParseSinks([]byte(`
sinks:
  useless:
    timeout: 5s
`))
	if err != nil {
		t.Fatalf("ParseSinks: %v", err)
	}
	if err := table.Resolve(os.LookupEnv, os.ReadFile); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if _, err := SetupSinks(context.Background(), Config{Sinks: table}); err == nil {
		t.Fatal("SetupSinks with an endpoint-less sink = nil error, want one naming the sink")
	} else if !strings.Contains(err.Error(), "useless") {
		t.Errorf("error %q does not name the sink", err)
	}
}

// TestSinksForWarnsOnceForUnknownSink is fix 4's contract: WithLogger attaches
// a logger that For uses to warn about an undeclared sink name, at most once
// no matter how many times the same name is asked for.
func TestSinksForWarnsOnceForUnknownSink(t *testing.T) {
	sinks, err := SetupSinks(context.Background(), Config{})
	if err != nil {
		t.Fatalf("SetupSinks: %v", err)
	}
	var buf strings.Builder
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	sinks.WithLogger(logger)

	for i := 0; i < 3; i++ {
		sinks.For("bogus")
	}
	got := strings.Count(buf.String(), "telemetry sink not declared")
	if got != 1 {
		t.Errorf("got %d Warn lines for the same unknown sink, want 1:\n%s", got, buf.String())
	}
	if !strings.Contains(buf.String(), "sink=bogus") {
		t.Errorf("Warn line = %q, want it to name the sink", buf.String())
	}

	// A different unknown name gets its own, separate warning.
	sinks.For("other-bogus")
	if got := strings.Count(buf.String(), "telemetry sink not declared"); got != 2 {
		t.Errorf("got %d Warn lines after a second unknown name, want 2", got)
	}
}

// TestSinksHas confirms Has distinguishes a declared sink (default or named)
// from an undeclared one, independent of For's own fallback.
func TestSinksHas(t *testing.T) {
	table, err := ParseSinks([]byte("sinks:\n  b:\n    endpoint: http://127.0.0.1:1\n"))
	if err != nil {
		t.Fatalf("ParseSinks: %v", err)
	}
	if err := table.Resolve(os.LookupEnv, os.ReadFile); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	sinks, err := SetupSinks(context.Background(), Config{Sinks: table})
	if err != nil {
		t.Fatalf("SetupSinks: %v", err)
	}
	for _, tc := range []struct {
		name string
		want bool
	}{
		{"", true},
		{DefaultSink, true},
		{"b", true},
		{"does-not-exist", false},
	} {
		if got := sinks.Has(tc.name); got != tc.want {
			t.Errorf("Has(%q) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestSinksShutdownRunsConcurrently is fix 5's contract: Shutdown must not
// serialize one sink's shutdown behind another's.
//
// Each sink's shutdown reports that it has started and then waits for the
// other's start. Run together, both are in flight at once and both return; run
// one after the other, the first waits for a start that cannot come until it
// returns, and gives up with an error naming that. Nothing is timed: a slow
// host delays both starts and changes no outcome, and the wait's own bound is
// only how long a serialized Shutdown takes to say so.
func TestSinksShutdownRunsConcurrently(t *testing.T) {
	started := make(chan struct{}, 2)
	bothStarted := make(chan struct{})
	go func() {
		<-started
		<-started
		close(bothStarted)
	}()
	rendezvous := func(context.Context) error {
		started <- struct{}{}
		select {
		case <-bothStarted:
			return nil
		case <-time.After(10 * time.Second):
			return errors.New("the other sink's shutdown had not started: Shutdown runs them one after the other")
		}
	}
	def := noopProviders(Config{})
	def.shutdowns = append(def.shutdowns, rendezvous)
	named := noopProviders(Config{})
	named.shutdowns = append(named.shutdowns, rendezvous)
	sinks := newSinksForTest(def, map[string]*Providers{"b": named})

	if err := sinks.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}
