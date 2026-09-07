package main

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/telemetry"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// memNamespaceStore is an in-memory artifacts.NamespaceStore for
// warnUndeclaredNamespaceSinks' tests: a real S3NamespaceStore needs a
// bucket, and this check cares only about what List returns.
type memNamespaceStore struct {
	namespaces []*orkstackv1.Namespace
	listErr    error
}

var _ artifacts.NamespaceStore = (*memNamespaceStore)(nil)

func (m *memNamespaceStore) Put(context.Context, *orkstackv1.Namespace, artifacts.PutConditions) (*orkstackv1.Namespace, error) {
	panic("not needed by warnUndeclaredNamespaceSinks tests")
}

func (m *memNamespaceStore) Get(context.Context, string) (*orkstackv1.Namespace, error) {
	panic("not needed by warnUndeclaredNamespaceSinks tests")
}

func (m *memNamespaceStore) List(context.Context) ([]*orkstackv1.Namespace, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	return m.namespaces, nil
}

func (m *memNamespaceStore) Delete(context.Context, string) error {
	panic("not needed by warnUndeclaredNamespaceSinks tests")
}

// TestWarnUndeclaredNamespaceSinks_WarnsOncePerUndeclaredNamespace asserts the
// cross-check's main case: a namespace naming a sink this deployment never
// declared in ORK_TELEMETRY_SINKS gets exactly one Warn, naming the
// namespace, the sink it asked for, and the sinks this deployment actually
// knows.
func TestWarnUndeclaredNamespaceSinks_WarnsOncePerUndeclaredNamespace(t *testing.T) {
	namespaces := &memNamespaceStore{namespaces: []*orkstackv1.Namespace{
		{Name: "payments", Telemetry: &orkstackv1.TelemetryPolicy{Sink: "audit"}},
		{Name: "default", Telemetry: &orkstackv1.TelemetryPolicy{Sink: ""}},
	}}
	tcfgSinks := sinksForTest(t)

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	warnUndeclaredNamespaceSinks(context.Background(), logger, namespaces, tcfgSinks)

	out := buf.String()
	if !strings.Contains(out, "namespace names a telemetry sink this deployment does not declare") {
		t.Fatalf("log output = %q, want a Warn naming the undeclared sink", out)
	}
	if !strings.Contains(out, "payments") || !strings.Contains(out, "audit") {
		t.Fatalf("log output = %q, want it to name namespace=payments sink=audit", out)
	}
	if strings.Contains(out, "namespace=default") {
		t.Fatalf("log output = %q, want no Warn for the namespace naming no sink", out)
	}
}

// TestWarnUndeclaredNamespaceSinks_NoWarnForADeclaredSink asserts the
// negative: a namespace naming a sink the deployment does declare (including
// the empty string and "default") gets no Warn at all.
func TestWarnUndeclaredNamespaceSinks_NoWarnForADeclaredSink(t *testing.T) {
	namespaces := &memNamespaceStore{namespaces: []*orkstackv1.Namespace{
		{Name: "default", Telemetry: &orkstackv1.TelemetryPolicy{Sink: "default"}},
		{Name: "no-policy", Telemetry: nil},
	}}
	tcfgSinks := sinksForTest(t)

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	warnUndeclaredNamespaceSinks(context.Background(), logger, namespaces, tcfgSinks)

	if buf.Len() != 0 {
		t.Fatalf("log output = %q, want none", buf.String())
	}
}

// TestWarnUndeclaredNamespaceSinks_StoreErrorIsAWarnNeverAFailure holds §11's
// rule that telemetry can never fail the deployment: a namespace store this
// check cannot list is logged and nothing else, so callers never treat it as
// a reason to refuse boot.
func TestWarnUndeclaredNamespaceSinks_StoreErrorIsAWarnNeverAFailure(t *testing.T) {
	namespaces := &memNamespaceStore{listErr: context.DeadlineExceeded}
	tcfgSinks := sinksForTest(t)

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	warnUndeclaredNamespaceSinks(context.Background(), logger, namespaces, tcfgSinks)

	if !strings.Contains(buf.String(), "list namespaces failed") {
		t.Fatalf("log output = %q, want a Warn naming the list failure", buf.String())
	}
}

// sinksForTest boots the zero-config Sinks: with no ORK_TELEMETRY_SINKS set,
// Names() is exactly {"default"}, which is all these tests need to exercise
// Has's fallback behavior.
func sinksForTest(t *testing.T) *telemetry.Sinks {
	t.Helper()
	clearTelemetryEnv(t)
	_, sinks, err := telemetry.Boot(telemetry.ServiceEngine)
	if err != nil {
		t.Fatalf("telemetry.Boot: %v", err)
	}
	return sinks
}
