package api_test

import (
	"context"
	"strings"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// telemetryService builds a Service the same way namespacedService does, but
// with a declared sink table — the shape a namespace's telemetry.sink is
// validated against (internal/api.validNamespaceTelemetry).
func telemetryService(t *testing.T, reg *fakeRegistry, sinks ...string) *api.Service {
	t.Helper()
	return api.NewService(api.Config{
		Orkflows:       engine.NewMemoryOrkflowStore(),
		Namespaces:     reg,
		Runs:           engine.NewRunStore(),
		Bus:            engine.NewEventBus(),
		Starter:        &fakeStarter{},
		TelemetrySinks: sinks,
	})
}

// ---------------------------------------------------------------------------
// ListTelemetrySinks
// ---------------------------------------------------------------------------

// TestListTelemetrySinks_AlwaysIncludesDefault: an engine with no declared
// table still answers with "default" — the OTEL_* environment is always the
// default destination, declared or not.
func TestListTelemetrySinks_AlwaysIncludesDefault(t *testing.T) {
	svc := telemetryService(t, newFakeRegistry())
	resp, err := svc.ListTelemetrySinks(context.Background(),
		connect.NewRequest(&orkstackv1.ListTelemetrySinksRequest{}))
	if err != nil {
		t.Fatalf("ListTelemetrySinks: %v", err)
	}
	if got := resp.Msg.GetNames(); len(got) != 1 || got[0] != "default" {
		t.Errorf("names = %v, want [default]", got)
	}
}

// TestListTelemetrySinks_ReportsTheConfiguredTableSortedAndDeduplicated.
func TestListTelemetrySinks_ReportsTheConfiguredTableSortedAndDeduplicated(t *testing.T) {
	svc := telemetryService(t, newFakeRegistry(), "datadog", "default", "honeycomb")
	resp, err := svc.ListTelemetrySinks(context.Background(),
		connect.NewRequest(&orkstackv1.ListTelemetrySinksRequest{}))
	if err != nil {
		t.Fatalf("ListTelemetrySinks: %v", err)
	}
	want := []string{"datadog", "default", "honeycomb"}
	got := resp.Msg.GetNames()
	if len(got) != len(want) {
		t.Fatalf("names = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("names[%d] = %q, want %q (names = %v)", i, got[i], want[i], got)
		}
	}
}

// ---------------------------------------------------------------------------
// CreateNamespace / UpdateNamespace: telemetry validation
// ---------------------------------------------------------------------------

// TestCreateNamespace_AcceptsAKnownTelemetrySink.
func TestCreateNamespace_AcceptsAKnownTelemetrySink(t *testing.T) {
	svc := telemetryService(t, newFakeRegistry(), "datadog")
	resp, err := svc.CreateNamespace(context.Background(), connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{
			Name:      "payments",
			Telemetry: &orkstackv1.TelemetryPolicy{Sink: "datadog"},
		},
	}))
	if err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	if got := resp.Msg.GetNamespace().GetTelemetry().GetSink(); got != "datadog" {
		t.Errorf("stored sink = %q, want datadog", got)
	}
}

// TestCreateNamespace_RefusesAnUnknownTelemetrySink names the sinks that do
// exist, the same shape validNamespaceRetention's ceiling refusal takes.
func TestCreateNamespace_RefusesAnUnknownTelemetrySink(t *testing.T) {
	svc := telemetryService(t, newFakeRegistry(), "datadog")
	_, err := svc.CreateNamespace(context.Background(), connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{
			Name:      "payments",
			Telemetry: &orkstackv1.TelemetryPolicy{Sink: "honeycomb"},
		},
	}))
	if err == nil {
		t.Fatal("creating a namespace with an undeclared telemetry sink succeeded")
	}
	if code := connect.CodeOf(err); code != connect.CodeInvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", code)
	}
	for _, want := range []string{"honeycomb", "default", "datadog"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err, want)
		}
	}
}

// TestCreateNamespace_RefusesAReservedAttributePrefix: ork., service. and
// host. are the platform's own namespaces (internal/engine.telemetryFor and
// OpenTelemetry's resource attributes); a namespace admin cannot shadow them.
func TestCreateNamespace_RefusesAReservedAttributePrefix(t *testing.T) {
	for _, prefix := range []string{"ork.", "service.", "host."} {
		t.Run(prefix, func(t *testing.T) {
			svc := telemetryService(t, newFakeRegistry())
			_, err := svc.CreateNamespace(context.Background(), connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
				Namespace: &orkstackv1.Namespace{
					Name: "payments",
					Telemetry: &orkstackv1.TelemetryPolicy{
						Attributes: map[string]string{prefix + "x": "1"},
					},
				},
			}))
			if err == nil {
				t.Fatalf("creating a namespace with attribute key %q succeeded", prefix+"x")
			}
			if code := connect.CodeOf(err); code != connect.CodeInvalidArgument {
				t.Errorf("code = %v, want InvalidArgument", code)
			}
			if !strings.Contains(err.Error(), prefix) {
				t.Errorf("error %q does not name the reserved prefix %q", err, prefix)
			}
		})
	}
}

// TestCreateNamespace_RefusesTooManyTelemetryAttributes: the same 16-entry
// bound internal/dsl.ValidateAttributeMap enforces for every bounded
// string-map in the tree.
func TestCreateNamespace_RefusesTooManyTelemetryAttributes(t *testing.T) {
	attrs := make(map[string]string, 17)
	for i := 0; i < 17; i++ {
		attrs[keyFor(i)] = "v"
	}
	svc := telemetryService(t, newFakeRegistry())
	_, err := svc.CreateNamespace(context.Background(), connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{
			Name:      "payments",
			Telemetry: &orkstackv1.TelemetryPolicy{Attributes: attrs},
		},
	}))
	if err == nil {
		t.Fatal("creating a namespace with 17 telemetry attributes succeeded")
	}
	if code := connect.CodeOf(err); code != connect.CodeInvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", code)
	}
	if !strings.Contains(err.Error(), "16") {
		t.Errorf("error %q does not name the 16-entry limit", err)
	}
}

func keyFor(i int) string {
	return string(rune('a'+i%26)) + "attr"
}

// TestCreateNamespace_RefusesAttributeValueWithControlCharacter: a namespace
// telemetry attribute is operator/admin input rendered verbatim in terminals
// (`orkctl namespace get`) and the UI's namespace page, so a value carrying a
// control character (an embedded newline splitting a table row, an escape
// sequence) is refused rather than stored and rendered as-is.
func TestCreateNamespace_RefusesAttributeValueWithControlCharacter(t *testing.T) {
	svc := telemetryService(t, newFakeRegistry())
	_, err := svc.CreateNamespace(context.Background(), connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{
			Name: "payments",
			Telemetry: &orkstackv1.TelemetryPolicy{
				Attributes: map[string]string{"team": "payments\ndrop table"},
			},
		},
	}))
	if err == nil {
		t.Fatal("creating a namespace with a control character in a telemetry attribute value succeeded")
	}
	if code := connect.CodeOf(err); code != connect.CodeInvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", code)
	}
	if !strings.Contains(err.Error(), "control character") {
		t.Errorf("error %q does not name the control character rule", err)
	}
}

// TestUpdateNamespace_RefusesAnUnknownTelemetrySink is the update-path half
// of TestCreateNamespace_RefusesAnUnknownTelemetrySink.
func TestUpdateNamespace_RefusesAnUnknownTelemetrySink(t *testing.T) {
	svc := telemetryService(t, newFakeRegistry(), "datadog")
	created, err := svc.CreateNamespace(context.Background(), connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{Name: "payments"},
	}))
	if err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	edited := created.Msg.GetNamespace()
	edited.Telemetry = &orkstackv1.TelemetryPolicy{Sink: "honeycomb"}
	_, err = svc.UpdateNamespace(context.Background(),
		connect.NewRequest(&orkstackv1.UpdateNamespaceRequest{Namespace: edited}))
	if err == nil {
		t.Fatal("updating a namespace onto an undeclared telemetry sink succeeded")
	}
	if code := connect.CodeOf(err); code != connect.CodeInvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", code)
	}
}

// TestUpdateNamespace_ToleratesAStoredSinkTheDeploymentNoLongerDeclares is the
// operator-facing half of the split: an operator can remove a sink from
// ORK_TELEMETRY_SINKS at any time (docs/DEPLOY.md promises the namespaces
// that named it merely fall back to the default sink), and a namespace whose
// stored document still names that sink must stay editable — description,
// pools, retention, labels — same as any other namespace. Only a change TO
// another unknown sink is refused; leaving the stale sink untouched is not.
func TestUpdateNamespace_ToleratesAStoredSinkTheDeploymentNoLongerDeclares(t *testing.T) {
	reg := newFakeRegistry()
	svc := telemetryService(t, reg, "datadog")
	created, err := svc.CreateNamespace(context.Background(), connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{
			Name:      "payments",
			Telemetry: &orkstackv1.TelemetryPolicy{Sink: "datadog"},
		},
	}))
	if err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	// The operator removes "datadog" from ORK_TELEMETRY_SINKS: reopen the
	// service with a table that no longer declares it, sharing the same
	// registry so the stored document (still naming "datadog") survives.
	svc = telemetryService(t, reg)

	edited := created.Msg.GetNamespace()
	edited.Description = "now paged to #payments-oncall"
	resp, err := svc.UpdateNamespace(context.Background(),
		connect.NewRequest(&orkstackv1.UpdateNamespaceRequest{Namespace: edited}))
	if err != nil {
		t.Fatalf("updating a namespace whose stored sink is no longer declared: %v", err)
	}
	if got := resp.Msg.GetNamespace().GetDescription(); got != "now paged to #payments-oncall" {
		t.Errorf("description = %q, want the edit to have taken", got)
	}
	if got := resp.Msg.GetNamespace().GetTelemetry().GetSink(); got != "datadog" {
		t.Errorf("sink = %q, want the stale sink name preserved, not silently rewritten", got)
	}
}

// TestUpdateNamespace_RefusesChangingToAnotherUnknownSink: a stored sink the
// deployment no longer declares is tolerated unchanged, but a caller who asks
// to change it to some OTHER name is held to the same existence rule create
// always enforces — this is not a general escape hatch for the sink field.
func TestUpdateNamespace_RefusesChangingToAnotherUnknownSink(t *testing.T) {
	reg := newFakeRegistry()
	svc := telemetryService(t, reg, "datadog")
	created, err := svc.CreateNamespace(context.Background(), connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{
			Name:      "payments",
			Telemetry: &orkstackv1.TelemetryPolicy{Sink: "datadog"},
		},
	}))
	if err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	svc = telemetryService(t, reg)

	edited := created.Msg.GetNamespace()
	edited.Telemetry = &orkstackv1.TelemetryPolicy{Sink: "honeycomb"}
	_, err = svc.UpdateNamespace(context.Background(),
		connect.NewRequest(&orkstackv1.UpdateNamespaceRequest{Namespace: edited}))
	if err == nil {
		t.Fatal("updating a namespace onto a different undeclared telemetry sink succeeded")
	}
	if code := connect.CodeOf(err); code != connect.CodeInvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", code)
	}
	if !strings.Contains(err.Error(), "honeycomb") {
		t.Errorf("error %q does not name the refused sink %q", err, "honeycomb")
	}
}

// ---------------------------------------------------------------------------
// difference reporting
// ---------------------------------------------------------------------------

// TestCreateNamespace_RepeatWithADifferentTelemetryPolicyReportsWhichField
// exercises specDifference's telemetry half against each of the three
// sub-fields in turn.
func TestCreateNamespace_RepeatWithADifferentTelemetryPolicyReportsWhichField(t *testing.T) {
	tests := []struct {
		name    string
		first   *orkstackv1.TelemetryPolicy
		second  *orkstackv1.TelemetryPolicy
		wantErr string
	}{
		{
			name:    "sink",
			first:   &orkstackv1.TelemetryPolicy{Sink: "datadog"},
			second:  &orkstackv1.TelemetryPolicy{Sink: "honeycomb"},
			wantErr: "telemetry.sink",
		},
		{
			name:    "task_logs",
			first:   &orkstackv1.TelemetryPolicy{TaskLogs: orkstackv1.TaskLogExport_TASK_LOG_EXPORT_ENABLED},
			second:  &orkstackv1.TelemetryPolicy{TaskLogs: orkstackv1.TaskLogExport_TASK_LOG_EXPORT_DISABLED},
			wantErr: "telemetry.task_logs",
		},
		{
			name:    "attributes",
			first:   &orkstackv1.TelemetryPolicy{Attributes: map[string]string{"team": "a"}},
			second:  &orkstackv1.TelemetryPolicy{Attributes: map[string]string{"team": "b"}},
			wantErr: "telemetry.attributes",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc := telemetryService(t, newFakeRegistry(), "datadog", "honeycomb")
			if _, err := svc.CreateNamespace(context.Background(), connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
				Namespace: &orkstackv1.Namespace{Name: "payments", Telemetry: tc.first},
			})); err != nil {
				t.Fatalf("first create: %v", err)
			}
			_, err := svc.CreateNamespace(context.Background(), connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
				Namespace: &orkstackv1.Namespace{Name: "payments", Telemetry: tc.second},
			}))
			if err == nil {
				t.Fatal("re-creating with a different telemetry policy succeeded")
			}
			if code := connect.CodeOf(err); code != connect.CodeAlreadyExists {
				t.Errorf("code = %v, want AlreadyExists", code)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not name %q", err, tc.wantErr)
			}
		})
	}
}

// TestCreateNamespace_IsIdempotentWithAnUnsetTelemetryPolicy: two creates
// that both leave telemetry unset are the same namespace said twice, not a
// collision — nil is nil is nil.
func TestCreateNamespace_IsIdempotentWithAnUnsetTelemetryPolicy(t *testing.T) {
	svc := telemetryService(t, newFakeRegistry())
	if _, err := svc.CreateNamespace(context.Background(), connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{Name: "payments"},
	})); err != nil {
		t.Fatalf("first create: %v", err)
	}
	resp, err := svc.CreateNamespace(context.Background(), connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{Name: "payments"},
	}))
	if err != nil {
		t.Fatalf("repeat create: %v", err)
	}
	if resp.Msg.GetCreated() {
		t.Error("the repeated create reports created=true; nothing was written")
	}
}
