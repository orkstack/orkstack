package main

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// ---------------------------------------------------------------------------
// namespace telemetry: flag parsing
// ---------------------------------------------------------------------------

// TestParseTaskLogsFlag is a table of every value --task-logs accepts, and
// the one shape it refuses: an invalid value is refused naming the accepted
// values, not with a bare "invalid input" a user has to guess at.
func TestParseTaskLogsFlag(t *testing.T) {
	tests := []struct {
		raw     string
		want    orkstackv1.TaskLogExport
		wantErr bool
	}{
		{raw: "", want: orkstackv1.TaskLogExport_TASK_LOG_EXPORT_UNSPECIFIED},
		{raw: "inherit", want: orkstackv1.TaskLogExport_TASK_LOG_EXPORT_UNSPECIFIED},
		{raw: "enabled", want: orkstackv1.TaskLogExport_TASK_LOG_EXPORT_ENABLED},
		{raw: "disabled", want: orkstackv1.TaskLogExport_TASK_LOG_EXPORT_DISABLED},
		{raw: "maybe", wantErr: true},
		{raw: "ENABLED", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.raw, func(t *testing.T) {
			got, err := parseTaskLogsFlag(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseTaskLogsFlag(%q) = %v, want an error", tc.raw, got)
				}
				for _, want := range []string{"enabled", "disabled", "inherit"} {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("error %q does not name accepted value %q", err, want)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("parseTaskLogsFlag(%q) unexpected error: %v", tc.raw, err)
			}
			if got != tc.want {
				t.Errorf("parseTaskLogsFlag(%q) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}

// TestParseKeyValuePairs_NamesTheOffendingFlag: --label and --telemetry-attribute
// share the parser, and each has to see its own flag name in the refusal, not
// a generic one that sends a user looking at the wrong flag.
func TestParseKeyValuePairs_NamesTheOffendingFlag(t *testing.T) {
	_, err := parseKeyValuePairs("telemetry-attribute", []string{"no-equals-sign"})
	if err == nil {
		t.Fatal("parseKeyValuePairs accepted a pair with no \"=\"")
	}
	if !strings.Contains(err.Error(), "telemetry-attribute") {
		t.Errorf("error %q does not name --telemetry-attribute", err)
	}
}

// namespaceTelemetryTestCmd builds a bare cobra.Command carrying exactly the
// three telemetry flags, so applyTelemetryFlags can be exercised without a
// full create/update command around it. Args mimics what cobra hands RunE
// after parsing: flags recognized as --flag=value or --flag value.
func namespaceTelemetryTestCmd(t *testing.T, args []string) (*cobra.Command, *string, *string, *[]string) {
	t.Helper()
	var sink, taskLogs string
	var attrs []string
	cmd := &cobra.Command{Use: "x", RunE: func(*cobra.Command, []string) error { return nil }}
	cmd.Flags().StringVar(&sink, "telemetry-sink", "", "")
	cmd.Flags().StringVar(&taskLogs, "task-logs", "inherit", "")
	cmd.Flags().StringArrayVar(&attrs, "telemetry-attribute", nil, "")
	if err := cmd.ParseFlags(args); err != nil {
		t.Fatalf("parse flags %v: %v", args, err)
	}
	return cmd, &sink, &taskLogs, &attrs
}

// TestApplyTelemetryFlags_OmittedFlagsKeepBase mirrors the rest of the
// namespace spec: a flag the caller did not pass leaves the corresponding
// field of an existing policy untouched, exactly like --description or
// --default-pool on `namespace update`.
func TestApplyTelemetryFlags_OmittedFlagsKeepBase(t *testing.T) {
	base := &orkstackv1.TelemetryPolicy{
		Sink:       "datadog",
		TaskLogs:   orkstackv1.TaskLogExport_TASK_LOG_EXPORT_ENABLED,
		Attributes: map[string]string{"team": "payments"},
	}
	cmd, sink, taskLogs, attrs := namespaceTelemetryTestCmd(t, nil)
	got, err := applyTelemetryFlags(cmd, base, *sink, *taskLogs, *attrs)
	if err != nil {
		t.Fatalf("applyTelemetryFlags: %v", err)
	}
	if got != base {
		t.Fatalf("applyTelemetryFlags with no flags changed returned a different policy: %+v", got)
	}
}

// TestApplyTelemetryFlags_OnlyOneFieldChanges: passing --telemetry-sink alone
// must not reset --telemetry-attribute back to empty — the per-field
// application applyTelemetryFlags exists for.
func TestApplyTelemetryFlags_OnlyOneFieldChanges(t *testing.T) {
	base := &orkstackv1.TelemetryPolicy{
		Sink:       "datadog",
		TaskLogs:   orkstackv1.TaskLogExport_TASK_LOG_EXPORT_ENABLED,
		Attributes: map[string]string{"team": "payments"},
	}
	cmd, sink, taskLogs, attrs := namespaceTelemetryTestCmd(t, []string{"--telemetry-sink", "default"})
	got, err := applyTelemetryFlags(cmd, base, *sink, *taskLogs, *attrs)
	if err != nil {
		t.Fatalf("applyTelemetryFlags: %v", err)
	}
	if got.GetSink() != "default" {
		t.Errorf("sink = %q, want default", got.GetSink())
	}
	if got.GetTaskLogs() != orkstackv1.TaskLogExport_TASK_LOG_EXPORT_ENABLED {
		t.Errorf("task_logs changed to %v though --task-logs was not passed", got.GetTaskLogs())
	}
	if got.GetAttributes()["team"] != "payments" {
		t.Errorf("attributes changed to %v though --telemetry-attribute was not passed", got.GetAttributes())
	}
}

// TestApplyTelemetryFlags_EmptySinkResetsToDefault: an explicit empty
// --telemetry-sink is the documented reset, distinct from omitting the flag.
func TestApplyTelemetryFlags_EmptySinkResetsToDefault(t *testing.T) {
	base := &orkstackv1.TelemetryPolicy{Sink: "datadog"}
	cmd, sink, taskLogs, attrs := namespaceTelemetryTestCmd(t, []string{"--telemetry-sink="})
	got, err := applyTelemetryFlags(cmd, base, *sink, *taskLogs, *attrs)
	if err != nil {
		t.Fatalf("applyTelemetryFlags: %v", err)
	}
	if got.GetSink() != "" {
		t.Errorf("sink = %q, want reset to \"\" (default)", got.GetSink())
	}
}

// TestApplyTelemetryFlags_NoFlagsAndNoBaseStaysNil: create with no telemetry
// flags must produce no policy at all, not an all-zero one — the difference
// between "inherited" and "explicitly asked for every default".
func TestApplyTelemetryFlags_NoFlagsAndNoBaseStaysNil(t *testing.T) {
	cmd, sink, taskLogs, attrs := namespaceTelemetryTestCmd(t, nil)
	got, err := applyTelemetryFlags(cmd, nil, *sink, *taskLogs, *attrs)
	if err != nil {
		t.Fatalf("applyTelemetryFlags: %v", err)
	}
	if got != nil {
		t.Fatalf("applyTelemetryFlags with no base and no flags = %+v, want nil", got)
	}
}

// TestApplyTelemetryFlags_InvalidTaskLogsPropagates: a bad --task-logs value
// is refused by applyTelemetryFlags itself, before any request leaves the
// process.
func TestApplyTelemetryFlags_InvalidTaskLogsPropagates(t *testing.T) {
	cmd, sink, _, attrs := namespaceTelemetryTestCmd(t, []string{"--task-logs", "maybe"})
	taskLogs := "maybe"
	if _, err := applyTelemetryFlags(cmd, nil, *sink, taskLogs, *attrs); err == nil {
		t.Fatal("applyTelemetryFlags accepted --task-logs maybe")
	}
}

// ---------------------------------------------------------------------------
// namespace telemetry: end to end through `namespace create` / `update`
// ---------------------------------------------------------------------------

// TestNamespaceCreate_TelemetryFlagsRoundTrip exercises the flags through the
// real create/get path: the sink this deployment declares ("default") is
// accepted, attributes and task-logs round-trip, and the rendering says so.
func TestNamespaceCreate_TelemetryFlagsRoundTrip(t *testing.T) {
	f := newFixture(t)

	created := f.run("namespace", "create", "payments",
		"--telemetry-sink", "default",
		"--task-logs", "enabled",
		"--telemetry-attribute", "team=payments",
	).requireExit(t, exitOK)
	created.requireContains(t, "Telemetry sink:", "Task logs:", "enabled", "team=payments")

	got := f.run("namespace", "get", "payments").requireExit(t, exitOK)
	got.requireContains(t, "Telemetry sink:", "Task logs:", "enabled", "team=payments")
}

// TestNamespaceCreate_RefusesAnUnknownTelemetrySink: the CLI passes the flag
// straight through, and the engine's own refusal (internal/api) is what the
// user sees — it names the sinks that do exist.
func TestNamespaceCreate_RefusesAnUnknownTelemetrySink(t *testing.T) {
	f := newFixture(t)

	outcome := f.run("namespace", "create", "payments", "--telemetry-sink", "datadog")
	if outcome.code == exitOK {
		t.Fatalf("creating a namespace with an undeclared telemetry sink succeeded:\n%s", outcome.stdout)
	}
	for _, want := range []string{"datadog", "default"} {
		if !strings.Contains(outcome.stderr, want) {
			t.Errorf("stderr does not name %q:\n%s", want, outcome.stderr)
		}
	}
}

// TestNamespaceCreate_RefusesAnInvalidTaskLogsValue is the CLI-level version
// of TestParseTaskLogsFlag: the refusal never reaches the engine.
func TestNamespaceCreate_RefusesAnInvalidTaskLogsValue(t *testing.T) {
	f := newFixture(t)

	outcome := f.run("namespace", "create", "payments", "--task-logs", "maybe")
	if outcome.code == exitOK {
		t.Fatalf("creating a namespace with --task-logs maybe succeeded:\n%s", outcome.stdout)
	}
	for _, want := range []string{"enabled", "disabled", "inherit"} {
		if !strings.Contains(outcome.stderr, want) {
			t.Errorf("stderr does not name accepted value %q:\n%s", want, outcome.stderr)
		}
	}
}

// TestNamespaceUpdate_TelemetrySinkAloneKeepsAttributes is the end-to-end
// version of TestApplyTelemetryFlags_OnlyOneFieldChanges: updating one
// telemetry field through the real update path must not clear another.
func TestNamespaceUpdate_TelemetrySinkAloneKeepsAttributes(t *testing.T) {
	f := newFixture(t)
	f.run("namespace", "create", "payments",
		"--telemetry-sink", "default",
		"--telemetry-attribute", "team=payments",
	).requireExit(t, exitOK)

	f.run("namespace", "update", "payments", "--telemetry-sink", "default").
		requireExit(t, exitOK)

	got := f.run("namespace", "get", "payments").requireExit(t, exitOK)
	got.requireContains(t, "team=payments")
}
