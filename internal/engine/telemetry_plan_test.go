package engine_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

const telemetryDocument = `
name: telemetry-plan
tasks:
  - id: work
    uses: shell
    with: {command: "echo work"}
`

// planTelemetry runs PoolResolver.Plan for the given namespace and orkflow
// document and returns just the plan's telemetry selection, or the error Plan
// returned.
func planTelemetry(t *testing.T, ns *orkstackv1.Namespace, document string) (*engine.TelemetrySelection, error) {
	t.Helper()
	wf := compile(t, document)
	resolver := &engine.PoolResolver{Probe: everyPoolLive{}}
	_, plan, err := resolver.Plan(context.Background(), ns, wf, false)
	if err != nil {
		return nil, err
	}
	return plan.Telemetry, nil
}

// TestPoolResolver_PlanTelemetry_NilWithNoPolicyAndNoLabels is the common
// case, and the one the golden-history fixture depends on: a namespace with
// no TelemetryPolicy and an orkflow with no labels produces a nil selection,
// so ExecuteTaskInput.Telemetry and Run.Telemetry stay unset exactly as they
// were before this field existed.
func TestPoolResolver_PlanTelemetry_NilWithNoPolicyAndNoLabels(t *testing.T) {
	telemetry, err := planTelemetry(t, &orkstackv1.Namespace{}, telemetryDocument)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if telemetry != nil {
		t.Fatalf("telemetry = %+v, want nil", telemetry)
	}
}

// TestPoolResolver_PlanTelemetry_PolicyOnly checks that a namespace's own
// policy travels onto the plan, stamped ork.ns.<key>, when the orkflow itself
// declares no labels.
func TestPoolResolver_PlanTelemetry_PolicyOnly(t *testing.T) {
	ns := &orkstackv1.Namespace{
		Telemetry: &orkstackv1.TelemetryPolicy{
			Sink:       "otel-prod",
			TaskLogs:   orkstackv1.TaskLogExport_TASK_LOG_EXPORT_ENABLED,
			Attributes: map[string]string{"team": "payments"},
		},
	}
	telemetry, err := planTelemetry(t, ns, telemetryDocument)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if telemetry == nil {
		t.Fatal("telemetry = nil, want the namespace's policy")
	}
	if telemetry.Sink != "otel-prod" {
		t.Errorf("Sink = %q, want %q", telemetry.Sink, "otel-prod")
	}
	if telemetry.TaskLogs != "TASK_LOG_EXPORT_ENABLED" {
		t.Errorf("TaskLogs = %q, want %q", telemetry.TaskLogs, "TASK_LOG_EXPORT_ENABLED")
	}
	if got, want := telemetry.Attributes["ork.ns.team"], "payments"; got != want {
		t.Errorf(`attributes["ork.ns.team"] = %q, want %q`, got, want)
	}
}

// TestPoolResolver_PlanTelemetry_LabelsOnly checks the other half: an
// orkflow's own labels travel stamped ork.label.<key> even when its
// namespace sets no policy at all.
func TestPoolResolver_PlanTelemetry_LabelsOnly(t *testing.T) {
	document := `
name: telemetry-plan
labels:
  x: "1"
tasks:
  - id: work
    uses: shell
    with: {command: "echo work"}
`
	telemetry, err := planTelemetry(t, &orkstackv1.Namespace{}, document)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if telemetry == nil {
		t.Fatal("telemetry = nil, want a selection carrying the orkflow's label")
	}
	if telemetry.Sink != "" {
		t.Errorf("Sink = %q, want empty (no namespace policy)", telemetry.Sink)
	}
	if got, want := telemetry.Attributes["ork.label.x"], "1"; got != want {
		t.Errorf(`attributes["ork.label.x"] = %q, want %q`, got, want)
	}
}

// TestPoolResolver_PlanTelemetry_BothSurviveWithNoCollision checks that a
// namespace's attribute and an orkflow's label of the SAME raw name — the one
// case an author might expect to collide — both survive the merge intact,
// because ork.ns.<key> and ork.label.<key> are disjoint by construction (no
// string starts with both "ork.ns." and "ork.label."). Nothing is silently
// dropped for sharing a name across the two sources.
func TestPoolResolver_PlanTelemetry_BothSurviveWithNoCollision(t *testing.T) {
	ns := &orkstackv1.Namespace{
		Telemetry: &orkstackv1.TelemetryPolicy{
			Attributes: map[string]string{"x": "namespace-value"},
		},
	}
	document := `
name: telemetry-plan
labels:
  x: "orkflow-value"
tasks:
  - id: work
    uses: shell
    with: {command: "echo work"}
`
	telemetry, err := planTelemetry(t, ns, document)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if telemetry == nil {
		t.Fatal("telemetry = nil, want a selection")
	}
	if got, want := telemetry.Attributes["ork.ns.x"], "namespace-value"; got != want {
		t.Errorf(`attributes["ork.ns.x"] = %q, want %q`, got, want)
	}
	if got, want := telemetry.Attributes["ork.label.x"], "orkflow-value"; got != want {
		t.Errorf(`attributes["ork.label.x"] = %q, want %q`, got, want)
	}
}

// TestPoolResolver_PlanTelemetry_LabelsCapAt32 checks the export boundary's
// own bound: an orkflow with more than 32 labels still plans successfully
// (labels are free-form and unbounded at the DSL layer — see
// TestCompile_Accepts40Labels in internal/dsl), and telemetryFor exports at
// most 32 of them, the ones sorted first by key.
func TestPoolResolver_PlanTelemetry_LabelsCapAt32(t *testing.T) {
	var labelLines strings.Builder
	for i := 0; i < 40; i++ {
		fmt.Fprintf(&labelLines, "  l%02d: \"v\"\n", i)
	}
	document := fmt.Sprintf(`
name: telemetry-plan
labels:
%s
tasks:
  - id: work
    uses: shell
    with: {command: "echo work"}
`, labelLines.String())

	telemetry, err := planTelemetry(t, &orkstackv1.Namespace{}, document)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if telemetry == nil {
		t.Fatal("telemetry = nil, want a selection carrying 32 of the orkflow's 40 labels")
	}
	if got, want := len(telemetry.Attributes), 32; got != want {
		t.Fatalf("exported %d label attributes, want %d", got, want)
	}
	// Sorted-key order keeps the first 32 (l00..l31), not an arbitrary 32.
	if _, ok := telemetry.Attributes["ork.label.l00"]; !ok {
		t.Errorf("attributes missing ork.label.l00, which sorts first")
	}
	if _, ok := telemetry.Attributes["ork.label.l31"]; !ok {
		t.Errorf("attributes missing ork.label.l31, the 32nd in sorted order")
	}
	if _, ok := telemetry.Attributes["ork.label.l32"]; ok {
		t.Errorf("attributes carry ork.label.l32, the 33rd label, want it dropped")
	}
}

// TestPoolResolver_PlanTelemetry_SanitizesLabelKeysAndValues checks that a
// label whose key is not already telemetry-attribute-shaped is folded rather
// than refused: "Team" (uppercase) and "app.kubernetes.io/name" (a slash,
// outside the attribute alphabet) both export cleanly.
func TestPoolResolver_PlanTelemetry_SanitizesLabelKeysAndValues(t *testing.T) {
	document := `
name: telemetry-plan
labels:
  Team: payments
  app.kubernetes.io/name: checkout
tasks:
  - id: work
    uses: shell
    with: {command: "echo work"}
`
	telemetry, err := planTelemetry(t, &orkstackv1.Namespace{}, document)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if got, want := telemetry.Attributes["ork.label.team"], "payments"; got != want {
		t.Errorf(`attributes["ork.label.team"] = %q, want %q`, got, want)
	}
	if got, want := telemetry.Attributes["ork.label.app.kubernetes.io_name"], "checkout"; got != want {
		t.Errorf(`attributes["ork.label.app.kubernetes.io_name"] = %q, want %q`, got, want)
	}
}

// TestPoolResolver_PlanTelemetry_LabelSanitizedKeyCollisionKeepsFirstSorted
// checks the collision rule directly: "Team" and "team" both fold to
// "ork.label.team", and the one that sorts first ("Team" < "team", capital
// letters sort before lowercase in byte order) wins.
func TestPoolResolver_PlanTelemetry_LabelSanitizedKeyCollisionKeepsFirstSorted(t *testing.T) {
	document := `
name: telemetry-plan
labels:
  Team: from-Team
  team: from-team
tasks:
  - id: work
    uses: shell
    with: {command: "echo work"}
`
	telemetry, err := planTelemetry(t, &orkstackv1.Namespace{}, document)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if got, want := telemetry.Attributes["ork.label.team"], "from-Team"; got != want {
		t.Errorf(`attributes["ork.label.team"] = %q, want %q (from the sorted-first key "Team")`, got, want)
	}
}

// TestPoolResolver_PlanTelemetry_LabelValueSanitized checks that a label
// value is truncated to 256 bytes and stripped of control characters at
// export, rather than carried onto the wire as-is.
func TestPoolResolver_PlanTelemetry_LabelValueSanitized(t *testing.T) {
	document := `
name: telemetry-plan
labels:
  note: "payments\ndrop table"
tasks:
  - id: work
    uses: shell
    with: {command: "echo work"}
`
	telemetry, err := planTelemetry(t, &orkstackv1.Namespace{}, document)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if got, want := telemetry.Attributes["ork.label.note"], "paymentsdrop table"; got != want {
		t.Errorf(`attributes["ork.label.note"] = %q, want %q`, got, want)
	}
}
