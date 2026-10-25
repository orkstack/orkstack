package telemetry

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseSinksHappyPath(t *testing.T) {
	doc := []byte(`
sinks:
  default:
    endpoint: http://otel-collector:4318
  datadog:
    endpoint: https://otlp.example
    logs_endpoint: https://http-intake.logs.datadoghq.com/api/v2/logs?ddsource=otlp
    traces_endpoint: https://trace.agent.datadoghq.com/api/v0.2/traces
    metrics_endpoint: https://api.datadoghq.com/api/v2/otlp/v1/metrics
    headers:
      DD-API-KEY: { source: "env:DD_API_KEY" }
      X-Scope-OrgID: { value: "tenant-a" }
    insecure: false
    timeout: 10s
`)
	table, err := ParseSinks(doc)
	if err != nil {
		t.Fatalf("ParseSinks: %v", err)
	}
	if table.Len() != 2 {
		t.Fatalf("Len() = %d, want 2", table.Len())
	}
	if !table.Has("default") || !table.Has("datadog") {
		t.Fatalf("Has() missing an expected sink; names=%v", table.Names())
	}
	if got, want := table.Names(), []string{"datadog", "default"}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("Names() = %v, want %v", got, want)
	}
	spec := table.sinks["datadog"]
	if spec.Endpoint != "https://otlp.example" {
		t.Errorf("Endpoint = %q", spec.Endpoint)
	}
	if spec.Timeout.String() != "10s" {
		t.Errorf("Timeout = %v, want 10s", spec.Timeout)
	}
	if len(spec.Headers) != 2 {
		t.Errorf("Headers = %v, want 2 entries", spec.Headers)
	}
}

func TestParseSinksEmptyDocument(t *testing.T) {
	table, err := ParseSinks(nil)
	if err != nil {
		t.Fatalf("ParseSinks(nil): %v", err)
	}
	if table.Len() != 0 {
		t.Errorf("Len() = %d, want 0", table.Len())
	}

	table, err = ParseSinks([]byte("   \n\t  "))
	if err != nil {
		t.Fatalf("ParseSinks(blank): %v", err)
	}
	if table.Len() != 0 {
		t.Errorf("Len() = %d, want 0", table.Len())
	}
}

func TestParseSinksUnknownKeyRefused(t *testing.T) {
	_, err := ParseSinks([]byte(`
sinks:
  default:
    endpoint: http://x
    bogus_field: true
`))
	if err == nil {
		t.Fatal("expected an error for an unknown key")
	}
}

func TestParseSinksBadName(t *testing.T) {
	for _, name := range []string{"Default", "1sink", "sink name", "-sink", ""} {
		doc := fmt.Sprintf("sinks:\n  %q:\n    endpoint: http://x\n", name)
		if _, err := ParseSinks([]byte(doc)); err == nil {
			t.Errorf("ParseSinks: sink name %q was accepted, want refused", name)
		}
	}
}

// A sink header names exactly one of value and source, and a source only a
// scheme the sinks support; each refusal names what it refused.
func TestParseSinksMalformedHeaderRefused(t *testing.T) {
	for name, tc := range map[string]struct{ header, names string }{
		"both value and source":    {header: `{ value: "a", source: "env:B" }`, names: "X-Test"},
		"neither value nor source": {header: `{}`, names: "X-Test"},
		"an unknown source scheme": {header: `{ source: "vault:secret/data" }`, names: "vault"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ParseSinks([]byte("sinks:\n  default:\n    endpoint: http://x\n    headers:\n      X-Test: " + tc.header + "\n"))
			if err == nil {
				t.Fatalf("ParseSinks accepted a header %s", tc.header)
			}
			if !strings.Contains(err.Error(), tc.names) {
				t.Errorf("error %q does not name %q", err, tc.names)
			}
		})
	}
}

func TestSinkTableResolveEnv(t *testing.T) {
	table, err := ParseSinks([]byte(`
sinks:
  default:
    endpoint: http://x
    headers:
      X-Test: { source: "env:SINKS_TEST_VAR" }
`))
	if err != nil {
		t.Fatalf("ParseSinks: %v", err)
	}
	lookup := func(key string) (string, bool) {
		if key == "SINKS_TEST_VAR" {
			return "secret-value", true
		}
		return "", false
	}
	if err := table.Resolve(lookup, os.ReadFile); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got := table.sinks["default"].resolved["X-Test"]; got != "secret-value" {
		t.Errorf("resolved[X-Test] = %q, want secret-value", got)
	}
}

func TestSinkTableResolveFileTrimsTrailingNewline(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "secret")
	if err := os.WriteFile(path, []byte("file-secret\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	table, err := ParseSinks([]byte(fmt.Sprintf(`
sinks:
  default:
    endpoint: http://x
    headers:
      X-Test: { source: "file:%s" }
`, path)))
	if err != nil {
		t.Fatalf("ParseSinks: %v", err)
	}
	if err := table.Resolve(os.LookupEnv, os.ReadFile); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got := table.sinks["default"].resolved["X-Test"]; got != "file-secret" {
		t.Errorf("resolved[X-Test] = %q, want %q (trailing newline trimmed)", got, "file-secret")
	}
}

func TestSinkTableResolveMissingEnvNamesEverything(t *testing.T) {
	table, err := ParseSinks([]byte(`
sinks:
  datadog:
    endpoint: http://x
    headers:
      DD-API-KEY: { source: "env:DD_API_KEY_DOES_NOT_EXIST" }
`))
	if err != nil {
		t.Fatalf("ParseSinks: %v", err)
	}
	lookup := func(string) (string, bool) { return "", false }
	err = table.Resolve(lookup, os.ReadFile)
	if err == nil {
		t.Fatal("expected an error for a missing environment variable")
	}
	msg := err.Error()
	for _, want := range []string{"datadog", "DD-API-KEY", "DD_API_KEY_DOES_NOT_EXIST"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error %q does not mention %q", msg, want)
		}
	}
}

func TestSinkTableStringRedacts(t *testing.T) {
	table, err := ParseSinks([]byte(`
sinks:
  datadog:
    endpoint: https://otlp.example
    headers:
      DD-API-KEY: { value: "super-secret-value" }
`))
	if err != nil {
		t.Fatalf("ParseSinks: %v", err)
	}
	if err := table.Resolve(func(string) (string, bool) { return "", false }, os.ReadFile); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	s := table.String()
	if !strings.Contains(s, "DD-API-KEY") {
		t.Errorf("String() = %q, want it to name the header", s)
	}
	if strings.Contains(s, "super-secret-value") {
		t.Errorf("String() = %q, leaked a header value", s)
	}
	if strings.Contains(fmt.Sprintf("%v", table), "super-secret-value") {
		t.Errorf("%%v leaked a header value")
	}
}

func TestSinkTableResolveNilAndEmptyAreNoOps(t *testing.T) {
	var nilTable *SinkTable
	if err := nilTable.Resolve(os.LookupEnv, os.ReadFile); err != nil {
		t.Errorf("Resolve on nil *SinkTable: %v", err)
	}
	var empty SinkTable
	if err := empty.Resolve(os.LookupEnv, os.ReadFile); err != nil {
		t.Errorf("Resolve on empty SinkTable: %v", err)
	}
}

func TestSinkTableHasLenNamesZeroValue(t *testing.T) {
	var table SinkTable
	if table.Len() != 0 {
		t.Errorf("Len() = %d, want 0", table.Len())
	}
	if table.Has("anything") {
		t.Errorf("Has() = true on zero value")
	}
	if names := table.Names(); len(names) != 0 {
		t.Errorf("Names() = %v, want empty", names)
	}
}

func TestSinkTableStringZeroValue(t *testing.T) {
	var table SinkTable
	if got := table.String(); !strings.Contains(got, "none") {
		t.Errorf("String() = %q, want it to say there are no sinks", got)
	}
}

// TestMergedHeadersEnvHeaderOnHeaderlessSink is fix 3's contract: a sink spec
// with no `headers:` at all still dials with whatever the environment's
// OTEL_EXPORTER_OTLP_*_HEADERS names — a spec is additive to the environment,
// never a reason to ignore it.
func TestMergedHeadersEnvHeaderOnHeaderlessSink(t *testing.T) {
	spec := &SinkSpec{Endpoint: "http://127.0.0.1:1"}
	lookup := lookupOf(map[string]string{
		"OTEL_EXPORTER_OTLP_HEADERS": "x-api-key=env-secret",
	})
	got := mergedHeaders(spec, "logs", lookup)
	if got["x-api-key"] != "env-secret" {
		t.Errorf("mergedHeaders = %v, want x-api-key=env-secret from the environment", got)
	}
}

// TestMergedHeadersSignalSpecificOverridesGeneric confirms the per-signal
// variable wins over the generic one for the same key — the OTel spec's own
// "more specific wins" rule.
func TestMergedHeadersSignalSpecificOverridesGeneric(t *testing.T) {
	lookup := lookupOf(map[string]string{
		"OTEL_EXPORTER_OTLP_HEADERS":      "x-api-key=generic",
		"OTEL_EXPORTER_OTLP_LOGS_HEADERS": "x-api-key=logs-specific",
	})
	got := mergedHeaders(nil, "logs", lookup)
	if got["x-api-key"] != "logs-specific" {
		t.Errorf("mergedHeaders[x-api-key] = %q, want %q (signal-specific overrides generic)",
			got["x-api-key"], "logs-specific")
	}
}

// TestMergedHeadersSpecOverridesEnv confirms a sink's own resolved `headers:`
// take precedence over anything the environment names for the same key — an
// operator naming a sink explicitly is the most specific layer of all.
func TestMergedHeadersSpecOverridesEnv(t *testing.T) {
	table, err := ParseSinks([]byte(`
sinks:
  datadog:
    endpoint: http://127.0.0.1:1
    headers:
      X-Api-Key: { value: "spec-value" }
`))
	if err != nil {
		t.Fatalf("ParseSinks: %v", err)
	}
	if err := table.Resolve(os.LookupEnv, os.ReadFile); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	spec := table.sinks["datadog"]
	lookup := lookupOf(map[string]string{"OTEL_EXPORTER_OTLP_HEADERS": "X-Api-Key=env-value"})
	got := mergedHeaders(&spec, "traces", lookup)
	if got["X-Api-Key"] != "spec-value" {
		t.Errorf("mergedHeaders[X-Api-Key] = %q, want %q (spec overrides env)", got["X-Api-Key"], "spec-value")
	}
}

// TestMergedHeadersPercentDecodesValues confirms a value carrying a
// percent-encoded character is decoded, the shape the OTel spec requires for
// this variable (a header value can itself contain "," or "=").
func TestMergedHeadersPercentDecodesValues(t *testing.T) {
	lookup := lookupOf(map[string]string{
		"OTEL_EXPORTER_OTLP_HEADERS": "authorization=Bearer%20abc%2Cdef",
	})
	got := mergedHeaders(nil, "metrics", lookup)
	if got["authorization"] != "Bearer abc,def" {
		t.Errorf("mergedHeaders[authorization] = %q, want %q", got["authorization"], "Bearer abc,def")
	}
}

// errorsJoinContains is a small helper making assertions on errors.Join
// output read the same as a single error's.
func errorsJoinContains(err error, substr string) bool {
	return err != nil && strings.Contains(err.Error(), substr)
}

func TestSinkTableResolveJoinsMultipleErrors(t *testing.T) {
	table, err := ParseSinks([]byte(`
sinks:
  one:
    endpoint: http://x
    headers:
      A: { source: "env:MISSING_A" }
  two:
    endpoint: http://x
    headers:
      B: { source: "env:MISSING_B" }
`))
	if err != nil {
		t.Fatalf("ParseSinks: %v", err)
	}
	err = table.Resolve(func(string) (string, bool) { return "", false }, os.ReadFile)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !errorsJoinContains(err, "MISSING_A") || !errorsJoinContains(err, "MISSING_B") {
		t.Errorf("joined error %q missing one of the two failures", err)
	}
	var joined interface{ Unwrap() []error }
	if !errors.As(err, &joined) {
		t.Errorf("error is not an errors.Join result: %T", err)
	}
}
