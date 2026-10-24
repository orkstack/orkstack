package api_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// A namespace name has one rule (artifacts: ^[a-z][a-z0-9-]*$, the DSL's slug
// rule), and a request that breaks it is a bad request — on every path, not
// only on the ones that write.
//
// Before this, the rule was checked at CreateNamespace and UpdateNamespace and
// nowhere else, so `Payments` reached the registry as a lookup. What came back
// depended on which registry was underneath: an in-memory one answered
// NotFound — "namespace "Payments" does not exist; these do: default", which
// reads as an empty namespace and sends the caller to create it — and the real
// store answered with its own refusal, mapped to Unavailable, which reads as
// "the deployment is unwell". Neither says what is true: the name cannot be a
// namespace, on this deployment or any other.

// invalidNamespaceNames is the corpus, one per way the rule can be broken:
// case, whitespace, a leading digit, and a separator that would address
// another prefix in the store.
var invalidNamespaceNames = []string{"Payments", "pay ments", "1payments", "pay/ments"}

// namespaceCarryingCalls is every RPC that takes a namespace, as a closure per
// call. A path missing here is a path where a malformed name still fails
// somewhere downstream.
func namespaceCarryingCalls(svc *api.Service) map[string]func(ctx context.Context, name string) error {
	return map[string]func(context.Context, string) error{
		"ListOrkflows": func(ctx context.Context, name string) error {
			_, err := svc.ListOrkflows(ctx,
				connect.NewRequest(&orkstackv1.ListOrkflowsRequest{Namespace: name}))
			return err
		},
		"GetOrkflow": func(ctx context.Context, name string) error {
			_, err := svc.GetOrkflow(ctx,
				connect.NewRequest(&orkstackv1.GetOrkflowRequest{Name: "deploy", Namespace: name}))
			return err
		},
		"ApplyOrkflow": func(ctx context.Context, name string) error {
			_, err := svc.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
				Yaml:      "apiVersion: orkstack/v1\nkind: Orkflow\nname: deploy\ntasks:\n  - id: build\n    uses: shell\n    with: {command: \"echo\"}\n",
				Namespace: name,
			}))
			return err
		},
		"ListRuns": func(ctx context.Context, name string) error {
			_, err := svc.ListRuns(ctx,
				connect.NewRequest(&orkstackv1.ListRunsRequest{Namespace: name}))
			return err
		},
		"SubmitRun": func(ctx context.Context, name string) error {
			_, err := svc.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
				OrkflowName: "deploy",
				Namespace:   name,
			}))
			return err
		},
		"GetRun": func(ctx context.Context, name string) error {
			_, err := svc.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{
				RunId:     "run-001",
				Namespace: name,
			}))
			return err
		},
		"CancelRun": func(ctx context.Context, name string) error {
			_, err := svc.CancelRun(ctx, connect.NewRequest(&orkstackv1.CancelRunRequest{
				RunId:     "run-001",
				Namespace: name,
			}))
			return err
		},
		"GetNamespace": func(ctx context.Context, name string) error {
			_, err := svc.GetNamespace(ctx,
				connect.NewRequest(&orkstackv1.GetNamespaceRequest{Name: name}))
			return err
		},
		"DeleteNamespace": func(ctx context.Context, name string) error {
			_, err := svc.DeleteNamespace(ctx,
				connect.NewRequest(&orkstackv1.DeleteNamespaceRequest{Name: name}))
			return err
		},
		"UpdateNamespace": func(ctx context.Context, name string) error {
			_, err := svc.UpdateNamespace(ctx, connect.NewRequest(&orkstackv1.UpdateNamespaceRequest{
				Namespace: &orkstackv1.Namespace{Name: name},
			}))
			return err
		},
		"CreateNamespace": func(ctx context.Context, name string) error {
			_, err := svc.CreateNamespace(ctx, connect.NewRequest(&orkstackv1.CreateNamespaceRequest{
				Namespace: &orkstackv1.Namespace{Name: name},
			}))
			return err
		},
		"ListPools": func(ctx context.Context, name string) error {
			_, err := svc.ListPools(ctx,
				connect.NewRequest(&orkstackv1.ListPoolsRequest{Namespace: name}))
			return err
		},
	}
}

// TestNamespaceName_IsCheckedAtTheBoundaryOfEveryPathThatTakesOne is the
// acceptance: one clear InvalidArgument, before the request reaches a store.
func TestNamespaceName_IsCheckedAtTheBoundaryOfEveryPathThatTakesOne(t *testing.T) {
	reg := newFakeRegistry(&orkstackv1.Namespace{Name: "payments"})
	svc, _ := namespacedService(t, reg)

	for call, invoke := range namespaceCarryingCalls(svc) {
		for _, name := range invalidNamespaceNames {
			err := invoke(context.Background(), name)
			if code := connect.CodeOf(err); code != connect.CodeInvalidArgument {
				t.Errorf("%s(namespace %q) code = %v, want InvalidArgument (err: %v)",
					call, name, code, err)
				continue
			}
			// The refusal has to be actionable: the name that was refused, and
			// the rule it broke. "invalid namespace" alone sends the reader to
			// the source.
			for _, want := range []string{name, "^[a-z][a-z0-9-]*$"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("%s(namespace %q) error %q does not name %q",
						call, name, err, want)
				}
			}
		}
	}
}

// validatingRegistry refuses a name the way artifacts.S3NamespaceStore does:
// the key cannot be rendered, so nothing is read. It exists to prove the
// boundary check is not merely covering for a lenient double — with a store
// that validates, the answer would otherwise be Unavailable, which blames the
// deployment for a caller's typo.
type validatingRegistry struct{ inner *fakeRegistry }

func (r validatingRegistry) Get(ctx context.Context, name string) (*orkstackv1.Namespace, error) {
	if _, err := artifacts.NamespaceKey(name); err != nil {
		return nil, fmt.Errorf("read namespace: %w", err)
	}
	return r.inner.Get(ctx, name)
}

func (r validatingRegistry) List(ctx context.Context) ([]*orkstackv1.Namespace, error) {
	return r.inner.List(ctx)
}

func (r validatingRegistry) ListNames(ctx context.Context) ([]string, error) {
	return r.inner.ListNames(ctx)
}

func (r validatingRegistry) Put(
	ctx context.Context,
	ns *orkstackv1.Namespace,
	conditions artifacts.PutConditions,
) (*orkstackv1.Namespace, error) {
	if _, err := artifacts.NamespaceKey(ns.GetName()); err != nil {
		return nil, fmt.Errorf("write namespace: %w", err)
	}
	return r.inner.Put(ctx, ns, conditions)
}

func (r validatingRegistry) Delete(ctx context.Context, name string) error {
	if _, err := artifacts.NamespaceKey(name); err != nil {
		return fmt.Errorf("delete namespace: %w", err)
	}
	return r.inner.Delete(ctx, name)
}

// TestNamespaceName_AStoreThatRefusesTheNameIsStillABadRequest covers the
// other half: whatever the boundary misses, a store's own ErrInvalidNamespace
// is a bad request and not an unavailable deployment.
func TestNamespaceName_AStoreThatRefusesTheNameIsStillABadRequest(t *testing.T) {
	reg := newFakeRegistry(&orkstackv1.Namespace{Name: "payments"})
	svc := api.NewService(api.Config{
		Orkflows:   engine.NewMemoryOrkflowStore(),
		Namespaces: validatingRegistry{inner: reg},
		Runs:       engine.NewRunStore(),
		Bus:        engine.NewEventBus(),
		Starter:    &fakeStarter{},
	})

	for call, invoke := range namespaceCarryingCalls(svc) {
		for _, name := range invalidNamespaceNames {
			err := invoke(context.Background(), name)
			if code := connect.CodeOf(err); code != connect.CodeInvalidArgument {
				t.Errorf("%s(namespace %q) against a validating store: code = %v, "+
					"want InvalidArgument (err: %v)", call, name, code, err)
			}
		}
	}
}
