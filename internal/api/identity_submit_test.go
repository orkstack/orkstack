package api

import (
	"context"
	"errors"
	"strings"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestCheckIdentityAtSubmit: at submit an audience the namespace no longer
// allows is refused failed_precondition; with identities_current_only a
// version that is not current is refused; a definition asking for nothing
// reads nothing.
func TestCheckIdentityAtSubmit(t *testing.T) {
	ctx := context.Background()
	store := engine.NewMemoryOrkflowStore()
	for _, v := range []string{"1", "2"} {
		def := identityDef("payments", "deploy", v, "sts")
		if _, _, err := store.Put(ctx, def, artifacts.HeadExpectation{}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.SetCurrentVersion(ctx, "payments", "deploy", "2", artifacts.CurrentExpectation{}); err != nil {
		t.Fatal(err)
	}
	ns := &orkstackv1.Namespace{Name: "payments"}
	v1, _ := store.Get(ctx, "payments", "deploy", "1")
	v2, _ := store.Get(ctx, "payments", "deploy", "2")
	allows := &stubPolicies{policy: artifacts.NamespacePolicy{Audiences: []string{"sts"}}}
	s := &Service{orkflows: store, namespacePolicies: allows}
	if err := s.checkIdentityAtSubmit(ctx, ns, v1, "payments"); err != nil {
		t.Fatalf("allowed: %v", err)
	}
	allows.policy.IdentitiesCurrentOnly = true
	if err := s.checkIdentityAtSubmit(ctx, ns, v1, "payments"); connect.CodeOf(err) != connect.CodeFailedPrecondition ||
		!strings.Contains(err.Error(), "not current") {
		t.Fatalf("an old version under current_only: %v", err)
	}
	if err := s.checkIdentityAtSubmit(ctx, ns, v2, "payments"); err != nil {
		t.Fatalf("the current version: %v", err)
	}
	allows.policy = artifacts.NamespacePolicy{Audiences: []string{"other"}}
	if err := s.checkIdentityAtSubmit(ctx, ns, v2, "payments"); connect.CodeOf(err) != connect.CodeFailedPrecondition ||
		!strings.Contains(err.Error(), identity.ReasonNotPermitted) {
		t.Fatalf("an audience no longer allowed: %v", err)
	}
	reads := allows.reads
	if err := s.checkIdentityAtSubmit(ctx, ns, &orkstackv1.Orkflow{Tasks: []*orkstackv1.Task{{Id: "plain"}}}, "payments"); err != nil || allows.reads != reads {
		t.Fatalf("a plain definition: %v, %d reads", err, allows.reads-reads)
	}
}

// fixedFleet answers the inventory with fixed Orkers, or an error.
type fixedFleet struct {
	engine.OrkerFleet
	orkers []engine.OrkerPresence
	scope  map[string]orkstackv1.StoreScope
	err    error
}

func (f fixedFleet) ObserveOrkers(context.Context) ([]engine.OrkerPresence, error) {
	return f.orkers, f.err
}
func (f fixedFleet) DescribeOrkerByID(_ context.Context, id string) (*orkstackv1.DescribeOrkerResult, error) {
	return &orkstackv1.DescribeOrkerResult{OrkerId: id, StoreScope: f.scope[id]}, nil
}

// TestCheckStoreScopedForIdentity: a run that holds an identity is refused
// IDENTITY_STORE_UNSCOPED while any live Orker is not confirmed scoped,
// naming it; report mode refuses nothing; an inventory that cannot be read
// is unavailable.
func TestCheckStoreScopedForIdentity(t *testing.T) {
	ctx := context.Background()
	scoped := fixedFleet{orkers: []engine.OrkerPresence{{Identity: "o1"}, {Identity: "o2"}},
		scope: map[string]orkstackv1.StoreScope{"o1": orkstackv1.StoreScope_STORE_SCOPE_SCOPED, "o2": orkstackv1.StoreScope_STORE_SCOPE_SCOPED}}
	if err := (&Service{fleet: scoped}).checkStoreScopedForIdentity(ctx); err != nil {
		t.Fatalf("all scoped: %v", err)
	}
	mixed := scoped
	mixed.scope = map[string]orkstackv1.StoreScope{"o1": orkstackv1.StoreScope_STORE_SCOPE_SCOPED, "o2": orkstackv1.StoreScope_STORE_SCOPE_UNSCOPED}
	err := (&Service{fleet: mixed}).checkStoreScopedForIdentity(ctx)
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), reasonStoreUnscoped) || !strings.Contains(err.Error(), "o2") {
		t.Fatalf("one unscoped: %v", err)
	}
	if err := (&Service{fleet: mixed, identityUnscopedOrkers: identity.UnscopedReport}).checkStoreScopedForIdentity(ctx); err != nil {
		t.Fatalf("report mode: %v", err)
	}
	down := fixedFleet{err: errors.New("frontend down")}
	if err := (&Service{fleet: down}).checkStoreScopedForIdentity(ctx); connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("an inventory that cannot be read: %v", err)
	}
}
