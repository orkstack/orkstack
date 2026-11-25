package main

import (
	"context"
	"errors"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestNewMinter_None: an engine that issues no task identity builds no
// mint, which the API refuses IDENTITY_ENGINE_REFUSED.
func TestNewMinter_None(t *testing.T) {
	m, records, err := newMinter(context.Background(), config{}, nil, nil, nil, nil)
	if m != nil || records != nil || err != nil {
		t.Fatalf("newMinter with no issuer = %v, %v, %v", m, records, err)
	}
}

// TestNamespacePolicy: the mint's step 7 reads the namespace's
// policy for the incarnation the run was admitted in; default's
// is read where the API writes it; an absent namespace, or one made again
// under its name, is gone, never "allows nothing".
func TestNamespacePolicy(t *testing.T) {
	ctx := context.Background()
	policies, err := artifacts.NewIncarnationStore(ctx, artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	created := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	if _, err := policies.SetNamespacePolicy(ctx, "payments", created, artifacts.NamespacePolicy{Audiences: []string{"sts"}}, 0, artifacts.PolicyWriter{}); err != nil {
		t.Fatal(err)
	}
	if _, err := policies.SetNamespacePolicy(ctx, "default", (*timestamppb.Timestamp)(nil).AsTime(), artifacts.NamespacePolicy{Audiences: []string{"vault"}}, 0, artifacts.PolicyWriter{}); err != nil {
		t.Fatal(err)
	}
	registry := namespacesOf{"payments": {Name: "payments", CreatedAt: timestamppb.New(created)}, "default": {Name: "default"}}
	paymentsID, err := policies.EnsureNamespaceID(ctx, "payments", created)
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]string{"payments": paymentsID, "default": artifacts.DefaultNamespaceID}
	for ns, want := range map[string]string{"payments": "sts", "default": "vault"} {
		got, err := namespacePolicy(ctx, registry, policies, ns, ids[ns])
		if err != nil || len(got.Audiences) != 1 || got.Audiences[0] != want {
			t.Errorf("%s: audiences = %v, %v; want %s", ns, got, err, want)
		}
	}
	if _, err := namespacePolicy(ctx, registry, policies, "absent", "ns_x"); !errors.Is(err, identity.ErrNamespaceGone) {
		t.Fatalf("an absent namespace: %v, want ErrNamespaceGone", err)
	}
	if _, err := namespacePolicy(ctx, registry, policies, "payments", "ns_an_earlier_one"); !errors.Is(err, identity.ErrNamespaceGone) {
		t.Fatalf("a namespace made again under its name: %v, want ErrNamespaceGone", err)
	}
}

// namespacesOf answers Get from a fixed set, as the registry would.
type namespacesOf map[string]*orkstackv1.Namespace

func (n namespacesOf) Get(_ context.Context, name string) (*orkstackv1.Namespace, error) {
	if ns, ok := n[name]; ok {
		return ns, nil
	}
	return nil, artifacts.ErrNotFound
}
