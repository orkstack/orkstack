package api_test

import (
	"context"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// TestGetRun_UnknownIDNamesTheNamespace: a run id that resolves nowhere is
// reported in the same words as a run of another namespace — "not found in
// namespace X" — so the message says where it looked, and the two cases stay
// indistinguishable to a caller who may not name the run's namespace.
func TestGetRun_UnknownIDNamesTheNamespace(t *testing.T) {
	svc := api.NewService(api.Config{
		Orkflows:   engine.NewMemoryOrkflowStore(),
		Namespaces: newFakeRegistry(&orkstackv1.Namespace{Name: "alpha"}),
		Runs:       engine.NewRunStore(),
		Bus:        engine.NewEventBus(),
		Starter:    &fakeStarter{},
	})
	_, err := svc.GetRun(context.Background(), connect.NewRequest(
		&orkstackv1.GetRunRequest{RunId: "ork-missing", Namespace: "alpha"}))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("GetRun = %v, want NotFound", err)
	}
	const want = `not_found: run "ork-missing" not found in namespace "alpha"`
	if err.Error() != want {
		t.Fatalf("GetRun error = %q, want %q", err.Error(), want)
	}
}
