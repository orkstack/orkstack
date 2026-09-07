package telemetry

import (
	"log/slog"
	"strings"
	"testing"
)

// lookupOf builds the (string) (string, bool) lookup configFrom wants from a
// plain map, so each test case reads as the environment it wants.
func lookupOf(env map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		v, ok := env[key]
		return v, ok
	}
}

func TestConfigFromZeroEnv(t *testing.T) {
	cfg, err := configFrom(ServiceOrker, lookupOf(nil))
	if err != nil {
		t.Fatalf("configFrom: %v", err)
	}
	if cfg.ServiceName != ServiceOrker {
		t.Errorf("ServiceName = %q, want %q", cfg.ServiceName, ServiceOrker)
	}
	if cfg.TracesEnabled() || cfg.MetricsEnabled() || cfg.LogsEnabled() {
		t.Errorf("zero env enabled something: %+v", cfg)
	}
	if cfg.LogFormat != LogFormatText {
		t.Errorf("LogFormat = %q, want text", cfg.LogFormat)
	}
	if cfg.LogLevel != slog.LevelInfo {
		t.Errorf("LogLevel = %v, want info", cfg.LogLevel)
	}
	if cfg.AdminAddr != "" {
		t.Errorf("AdminAddr = %q, want empty", cfg.AdminAddr)
	}
	if cfg.Enabled() {
		t.Errorf("Enabled() = true for zero env")
	}
}

// TestConfigSignalsEnabled covers SignalsEnabled's contract: true whenever
// any of traces, metrics or logs is on, false otherwise — unlike Enabled,
// which also counts a bare AdminAddr with no signal switched on.
func TestConfigSignalsEnabled(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  Config
		want bool
	}{
		{"zero value", Config{}, false},
		{"admin addr only", Config{AdminAddr: "127.0.0.1:0"}, false},
		{"traces", Config{TracesExporter: ExporterOTLP}, true},
		{"metrics", Config{MetricsExporters: []string{ExporterPrometheus}}, true},
		{"logs", Config{LogsExporter: ExporterOTLP}, true},
	} {
		if got := tc.cfg.SignalsEnabled(); got != tc.want {
			t.Errorf("%s: SignalsEnabled() = %v, want %v", tc.name, got, tc.want)
		}
	}
	// Enabled() must still count AdminAddr alone; SignalsEnabled() must not —
	// that's the entire reason the two methods differ.
	cfg := Config{AdminAddr: "127.0.0.1:0"}
	if !cfg.Enabled() {
		t.Error("Enabled() = false with AdminAddr set, want true")
	}
	if cfg.SignalsEnabled() {
		t.Error("SignalsEnabled() = true with only AdminAddr set, want false")
	}
}

func TestConfigFromMetricsExporters(t *testing.T) {
	cfg, err := configFrom(ServiceEngine, lookupOf(map[string]string{
		MetricsExporterVar: "prometheus,otlp,none",
		AdminAddrVar:       "127.0.0.1:9090",
	}))
	if err != nil {
		t.Fatalf("configFrom: %v", err)
	}
	if !cfg.PrometheusEnabled() || !cfg.OTLPMetricsEnabled() {
		t.Errorf("expected both exporters, got %+v", cfg.MetricsExporters)
	}
}

func TestConfigFromPrometheusRequiresAdminAddr(t *testing.T) {
	_, err := configFrom(ServiceEngine, lookupOf(map[string]string{
		MetricsExporterVar: "prometheus",
	}))
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), AdminAddrVar) {
		t.Errorf("error %q does not mention %s", err, AdminAddrVar)
	}
}

func TestConfigFromUnsupportedMetricsExporter(t *testing.T) {
	_, err := configFrom(ServiceEngine, lookupOf(map[string]string{
		MetricsExporterVar: "statsd",
	}))
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), MetricsExporterVar) || !strings.Contains(err.Error(), "statsd") {
		t.Errorf("error %q does not name the variable and value", err)
	}
	if !strings.Contains(err.Error(), "accepted") {
		t.Errorf("error %q does not list accepted exporters", err)
	}
}

func TestConfigFromLogFormat(t *testing.T) {
	cfg, err := configFrom(ServiceOrker, lookupOf(map[string]string{LogFormatVar: "JSON"}))
	if err != nil {
		t.Fatalf("configFrom: %v", err)
	}
	if cfg.LogFormat != LogFormatJSON {
		t.Errorf("LogFormat = %q, want json (case-insensitive)", cfg.LogFormat)
	}

	_, err = configFrom(ServiceOrker, lookupOf(map[string]string{LogFormatVar: "xml"}))
	if err == nil {
		t.Fatal("expected an error for xml log format")
	}
}

func TestConfigFromLogLevel(t *testing.T) {
	cfg, err := configFrom(ServiceOrker, lookupOf(map[string]string{LogLevelVar: "debug"}))
	if err != nil {
		t.Fatalf("configFrom: %v", err)
	}
	if cfg.LogLevel != slog.LevelDebug {
		t.Errorf("LogLevel = %v, want debug", cfg.LogLevel)
	}

	_, err = configFrom(ServiceOrker, lookupOf(map[string]string{LogLevelVar: "loud"}))
	if err == nil {
		t.Fatal("expected an error for an invalid log level")
	}
}

func TestConfigFromPprofEnabledInvalid(t *testing.T) {
	_, err := configFrom(ServiceOrker, lookupOf(map[string]string{PprofEnabledVar: "maybe"}))
	if err == nil {
		t.Fatal("expected an error for an invalid bool")
	}
}

func TestConfigFromHighCardinality(t *testing.T) {
	cfg, err := configFrom(ServiceOrker, lookupOf(map[string]string{HighCardinalityVar: "true"}))
	if err != nil {
		t.Fatalf("configFrom: %v", err)
	}
	if !cfg.HighCardinality {
		t.Errorf("HighCardinality = false, want true")
	}
}

func TestConfigFromServiceNameOverride(t *testing.T) {
	cfg, err := configFrom(ServiceOrker, lookupOf(map[string]string{ServiceNameVar: "custom-orker"}))
	if err != nil {
		t.Fatalf("configFrom: %v", err)
	}
	if cfg.ServiceName != "custom-orker" {
		t.Errorf("ServiceName = %q, want custom-orker", cfg.ServiceName)
	}
}

func TestConfigFromTracesExporter(t *testing.T) {
	cfg, err := configFrom(ServiceEngine, lookupOf(map[string]string{TracesExporterVar: "otlp"}))
	if err != nil {
		t.Fatalf("configFrom: %v", err)
	}
	if !cfg.TracesEnabled() {
		t.Errorf("TracesEnabled() = false, want true")
	}
}

func TestConfigFromMultipleErrorsJoined(t *testing.T) {
	_, err := configFrom(ServiceEngine, lookupOf(map[string]string{
		LogFormatVar:       "xml",
		LogLevelVar:        "loud",
		MetricsExporterVar: "statsd",
	}))
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{LogFormatVar, LogLevelVar, MetricsExporterVar} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("joined error %q missing %s", err, want)
		}
	}
}

func TestConfigFromSinks(t *testing.T) {
	cfg, err := configFrom(ServiceEngine, lookupOf(map[string]string{
		SinksVar: `
sinks:
  datadog:
    endpoint: https://otlp.example
    headers:
      X-Scope-OrgID: { value: "tenant-a" }
`,
	}))
	if err != nil {
		t.Fatalf("configFrom: %v", err)
	}
	if !cfg.Sinks.Has("datadog") {
		t.Fatalf("Sinks.Has(datadog) = false, want true; names=%v", cfg.Sinks.Names())
	}
	if cfg.Sinks.Len() != 1 {
		t.Errorf("Sinks.Len() = %d, want 1", cfg.Sinks.Len())
	}
}

func TestConfigFromSinksMalformedJoinsBootError(t *testing.T) {
	_, err := configFrom(ServiceEngine, lookupOf(map[string]string{
		SinksVar: `
sinks:
  Bad Name:
    endpoint: http://x
`,
	}))
	if err == nil {
		t.Fatal("expected an error for a non-slug sink name")
	}
	if !strings.Contains(err.Error(), SinksVar) {
		t.Errorf("error %q does not name %s", err, SinksVar)
	}
}

func TestConfigFromEnvResolvesSinkSecrets(t *testing.T) {
	t.Setenv(SinksVar, `
sinks:
  datadog:
    endpoint: https://otlp.example
    headers:
      DD-API-KEY: { source: "env:DD_API_KEY_MISSING_FOR_TEST" }
`)
	_, err := ConfigFromEnv(ServiceEngine)
	if err == nil {
		t.Fatal("expected ConfigFromEnv to surface an unresolved sink secret")
	}
	if !strings.Contains(err.Error(), "DD_API_KEY_MISSING_FOR_TEST") {
		t.Errorf("error %q does not name the missing environment variable", err)
	}
	if !strings.Contains(err.Error(), "datadog") {
		t.Errorf("error %q does not name the sink", err)
	}
	if strings.Contains(err.Error(), "DD-API-KEY") == false {
		t.Errorf("error %q does not name the header", err)
	}
}
