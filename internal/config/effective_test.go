package config

import (
	"strings"
	"testing"
)

func TestEffective_RedactsSecrets(t *testing.T) {
	path := writeTemp(t, "cfg.yaml", "storage:\n  s3:\n    secret_key: from-file-secret\n")
	doc, err := Load(path, BinaryOrk)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	out := doc.Effective(BinaryOrk, func(string) (string, bool) { return "", false })
	if strings.Contains(out, "from-file-secret") {
		t.Errorf("Effective output contains the secret value:\n%s", out)
	}
	if !strings.Contains(out, "storage.s3.secret_key: ***") {
		t.Errorf("Effective output = %q, want a redacted storage.s3.secret_key line", out)
	}
}

func TestEffective_Origins(t *testing.T) {
	path := writeTemp(t, "cfg.yaml", "telemetry:\n  log_level: debug\n")
	doc, err := Load(path, BinaryOrk)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	env := map[string]string{"ORK_LOG_FORMAT": "text-from-env"}
	lookup := func(k string) (string, bool) { v, ok := env[k]; return v, ok }
	out := doc.Effective(BinaryOrk, lookup)

	lines := map[string]string{}
	for _, line := range strings.Split(out, "\n") {
		if line == "" {
			continue
		}
		path := strings.SplitN(line, ":", 2)[0]
		lines[path] = line
	}

	// ORK_LOG_LEVEL came from the file.
	if l, ok := lines["telemetry.log_level"]; !ok || !strings.Contains(l, "origin=file") {
		t.Errorf("telemetry.log_level line = %q, want origin=file", l)
	}
	// ORK_LOG_FORMAT came from the environment, overriding any file value.
	if l, ok := lines["telemetry.log_format"]; !ok || !strings.Contains(l, "origin=env") {
		t.Errorf("telemetry.log_format line = %q, want origin=env", l)
	}
	// A key set nowhere reports its documented default.
	if l, ok := lines["temporal.address"]; !ok || !strings.Contains(l, "origin=default") {
		t.Errorf("temporal.address line = %q, want origin=default", l)
	}
}

// TestEffective_RedactsSinksHeaders proves a named sink's literal header
// value never appears in Effective output: telemetry.sinks is a
// KindDocument leaf, not KindSecret, so nothing upstream of Effective's own
// document-specific redaction would otherwise catch it.
func TestEffective_RedactsSinksHeaders(t *testing.T) {
	path := writeTemp(t, "cfg.yaml", `telemetry:
  sinks:
    grafana:
      endpoint: https://grafana:4318
      headers:
        authorization: super-secret-header-value
`)
	doc, err := Load(path, BinaryOrk)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	out := doc.Effective(BinaryOrk, func(string) (string, bool) { return "", false })
	if strings.Contains(out, "super-secret-header-value") {
		t.Errorf("Effective output contains the literal sink header value:\n%s", out)
	}
	if !strings.Contains(out, "***") {
		t.Errorf("Effective output = %q, want a redacted (***) marker for the sink header", out)
	}
	// The endpoint (not a secret) is untouched.
	if !strings.Contains(out, "grafana:4318") {
		t.Errorf("Effective output = %q, want the non-secret endpoint left intact", out)
	}
}

// TestEffective_RedactsSinksFileLeaf proves a {file: ...}-shaped leaf inside
// the sinks document (outside "headers") is also redacted, not just the
// headers block.
func TestEffective_RedactsSinksFileLeaf(t *testing.T) {
	secretFile := writeTemp(t, "sink-secret", "sink-secret-value\n")
	path := writeTemp(t, "cfg.yaml", `telemetry:
  sinks:
    grafana:
      endpoint: https://grafana:4318
      headers:
        authorization: {file: `+secretFile+`}
`)
	doc, err := Load(path, BinaryOrk)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	out := doc.Effective(BinaryOrk, func(string) (string, bool) { return "", false })
	if strings.Contains(out, secretFile) || strings.Contains(out, "sink-secret-value") {
		t.Errorf("Effective output leaks the {file:} leaf:\n%s", out)
	}
}

// TestEffective_ActivitiesDocument_NoRedaction proves the activities
// document — which never carries secrets — is printed as-is.
func TestEffective_ActivitiesDocument_NoRedaction(t *testing.T) {
	path := writeTemp(t, "cfg.yaml", "activities:\n  shell: ghcr.io/orkstack/activity-shell:0.2\n")
	doc, err := Load(path, BinaryOrk)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	out := doc.Effective(BinaryOrk, func(string) (string, bool) { return "", false })
	if !strings.Contains(out, "ghcr.io/orkstack/activity-shell:0.2") {
		t.Errorf("Effective output = %q, want the activities catalog reference intact", out)
	}
}

// TestEffective_QuotesSpecialValues proves Effective wraps a value in
// strconv.Quote when printing it bare would be ambiguous: containing '#'
// (which would otherwise swallow the rest of the line as Effective's own
// comment), ": " (which looks like a nested key), or leading/trailing
// whitespace.
func TestEffective_QuotesSpecialValues(t *testing.T) {
	cases := []struct {
		name, yaml, wantQuoted string
	}{
		{"hash", "temporal:\n  address: \"a#b\"\n", `"a#b"`},
		{"colon-space", "temporal:\n  address: \"a: b\"\n", `"a: b"`},
		{"leading space", "temporal:\n  address: \" a\"\n", `" a"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeTemp(t, "cfg.yaml", tc.yaml)
			doc, err := Load(path, BinaryOrk)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			out := doc.Effective(BinaryOrk, func(string) (string, bool) { return "", false })
			if !strings.Contains(out, tc.wantQuoted) {
				t.Errorf("Effective output = %q, want it to contain %q", out, tc.wantQuoted)
			}
		})
	}
}

func TestEffective_OnlyBinaryOwnKeys(t *testing.T) {
	path := writeTemp(t, "empty.yaml", "")
	doc, err := Load(path, BinaryOrk)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	out := doc.Effective(BinaryOrk, func(string) (string, bool) { return "", false })
	if strings.Contains(out, "orker.workdir") {
		t.Errorf("Effective(ork) output names an orker-only key:\n%s", out)
	}
	if !strings.Contains(out, "engine.http_addr") {
		t.Errorf("Effective(ork) output missing an ork key engine.http_addr:\n%s", out)
	}
}
