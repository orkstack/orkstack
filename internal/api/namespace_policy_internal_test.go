package api

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// etagRegistry keeps one etag per namespace, moved on every write, and
// honours IfMatch and IfAbsent.
type etagRegistry struct {
	mu   sync.Mutex
	docs map[string]*orkstackv1.Namespace
	seq  int
}

func (r *etagRegistry) Get(_ context.Context, name string) (*orkstackv1.Namespace, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ns, ok := r.docs[artifacts.ResolveNamespace(name)]; ok {
		return proto.Clone(ns).(*orkstackv1.Namespace), nil
	}
	return nil, fmt.Errorf("namespace %q: %w", name, artifacts.ErrNotFound)
}
func (r *etagRegistry) List(context.Context) ([]*orkstackv1.Namespace, error) { return nil, nil }
func (r *etagRegistry) ListNames(context.Context) ([]string, error)           { return nil, nil }
func (r *etagRegistry) Delete(context.Context, string) error                  { return nil }
func (r *etagRegistry) Put(_ context.Context, ns *orkstackv1.Namespace, cond artifacts.PutConditions) (*orkstackv1.Namespace, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	existing, ok := r.docs[ns.GetName()]
	if (cond.IfAbsent && ok) || (cond.IfMatch != "" && (!ok || existing.GetEtag() != cond.IfMatch)) {
		return nil, artifacts.ErrPreconditionFailed
	}
	r.seq++
	stored := proto.Clone(ns).(*orkstackv1.Namespace)
	stored.Etag = fmt.Sprintf("etag-%d", r.seq)
	r.docs[ns.GetName()] = stored
	return proto.Clone(stored).(*orkstackv1.Namespace), nil
}

// TestUpdateNamespace_DecidesThePolicyPermissionAgainstWhatItReplaces holds
// namespace.policy.update to the handler: a policy field is compared with
// the exact document the write replaces, so a caller holding only
// namespace.update never writes one, whatever changed between the
// interceptor's look and the write (decisions R3).
func TestUpdateNamespace_DecidesThePolicyPermissionAgainstWhatItReplaces(t *testing.T) {
	setup := func(t *testing.T, mode string, perms ...string) (*Service, *etagRegistry, context.Context) {
		t.Helper()
		reg := &etagRegistry{docs: map[string]*orkstackv1.Namespace{}}
		if _, err := reg.Put(context.Background(), &orkstackv1.Namespace{Name: "payments", AllowedPools: []string{"main"}}, artifacts.PutConditions{}); err != nil {
			t.Fatal(err)
		}
		s := NewService(Config{Orkflows: engine.NewMemoryOrkflowStore(), Runs: engine.NewRunStore(), Bus: engine.NewEventBus(), Namespaces: reg})
		a, _ := accessFor(t, mode, []*orkstackv1.Role{{Name: "updater", Permissions: perms}},
			&orkstackv1.Binding{Name: "u", Role: "updater", Scope: "namespace:payments", Subjects: []string{"user:corp:alice"}})
		s.access = a
		return s, reg, asAlice(mode)
	}
	update := func(s *Service, ctx context.Context, ns *orkstackv1.Namespace) error {
		_, err := s.UpdateNamespace(ctx, connect.NewRequest(&orkstackv1.UpdateNamespaceRequest{Namespace: ns}))
		return err
	}
	current := func(t *testing.T, reg *etagRegistry) *orkstackv1.Namespace {
		ns, err := reg.Get(context.Background(), "payments")
		if err != nil {
			t.Fatal(err)
		}
		return ns
	}

	t.Run("a description change needs namespace.update only", func(t *testing.T) {
		s, reg, ctx := setup(t, authn.ModeEnforce, "namespace.update")
		next := current(t, reg)
		next.Description = "payments team"
		if err := update(s, ctx, next); err != nil {
			t.Fatalf("UpdateNamespace: %v", err)
		}
	})
	t.Run("a policy field needs namespace.policy.update", func(t *testing.T) {
		s, reg, ctx := setup(t, authn.ModeEnforce, "namespace.update")
		next := current(t, reg)
		next.AllowedPools = []string{"main", "gpu"}
		if err := update(s, ctx, next); connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Fatalf("a policy change by a holder of namespace.update = %v; want permission_denied", err)
		}
		if got := current(t, reg).GetAllowedPools(); len(got) != 1 {
			t.Fatalf("allowed_pools = %v after a refused write", got)
		}
	})
	t.Run("a holder of namespace.policy.update changes it", func(t *testing.T) {
		s, reg, ctx := setup(t, authn.ModeEnforce, "namespace.update", "namespace.policy.update")
		next := current(t, reg)
		next.AllowedPools = []string{"main", "gpu"}
		if err := update(s, ctx, next); err != nil {
			t.Fatalf("UpdateNamespace: %v", err)
		}
	})
	t.Run("a policy field changed since the caller read it is not undone", func(t *testing.T) {
		s, reg, ctx := setup(t, authn.ModeEnforce, "namespace.update")
		read := current(t, reg)
		narrowed := current(t, reg)
		narrowed.AllowedPools = nil
		if _, err := reg.Put(context.Background(), narrowed, artifacts.PutConditions{IfMatch: narrowed.GetEtag()}); err != nil {
			t.Fatal(err)
		}
		read.Description = "only the description"
		if err := update(s, ctx, read); err == nil {
			t.Fatal("a description change carrying the old allowed_pools was written over a newer document")
		}
		if got := current(t, reg).GetAllowedPools(); len(got) != 0 {
			t.Fatalf("allowed_pools = %v; the narrowing was undone", got)
		}
	})
	t.Run("observe records and refuses nothing", func(t *testing.T) {
		s, reg, ctx := setup(t, authn.ModeObserve, "namespace.update")
		next := current(t, reg)
		next.AllowedPools = []string{"main", "gpu"}
		if err := update(s, ctx, next); err != nil {
			t.Fatalf("observe refused a policy change: %v", err)
		}
	})
}
