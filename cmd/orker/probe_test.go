package main

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/telemetry"
)

// TestStart_ProbeRunsBeforeAnyConfiguration: `orker probe` runs inside a
// probe container, which holds no configuration, no credentials and no
// environment of the Orker's. It reads none, sets none and serves nothing:
// its only output is the report.
func TestStart_ProbeRunsBeforeAnyConfiguration(t *testing.T) {
	lookup := func(k string) (string, bool) {
		t.Errorf("the probe read %s", k)
		return "", false
	}
	setenv := func(k, _ string) error {
		t.Errorf("the probe set %s", k)
		return nil
	}
	serve := func(*slog.Logger, telemetry.Config, *telemetry.Sinks, config) error {
		t.Error("the probe served")
		return nil
	}
	var out, errOut bytes.Buffer
	if code := start([]string{"probe", "view", "/dev/null"}, lookup, setenv, &out, &errOut, serve); code != 0 {
		t.Fatalf("exit %d: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), `"path":"/dev/null"`) || !strings.Contains(out.String(), `"kind":"char"`) {
		t.Fatalf("report = %s", out.String())
	}
	if code := start([]string{"probe"}, lookup, setenv, &out, &errOut, serve); code != 2 {
		t.Fatalf("a bare probe exited %d, want 2", code)
	}
}
