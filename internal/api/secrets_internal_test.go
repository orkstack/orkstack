package api

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// oneNamespace answers reads for a single stored namespace; writes are
// refused, since the apply below must never reach one.
type oneNamespace struct{ ns *orkstackv1.Namespace }

func (o oneNamespace) Get(_ context.Context, name string) (*orkstackv1.Namespace, error) {
	if artifacts.ResolveNamespace(name) == o.ns.GetName() {
		return o.ns, nil
	}
	if artifacts.ResolveNamespace(name) == artifacts.DefaultNamespace {
		return artifacts.DefaultNamespaceDocument(), nil
	}
	return nil, artifacts.ErrNotFound
}

func (o oneNamespace) List(context.Context) ([]*orkstackv1.Namespace, error) {
	return []*orkstackv1.Namespace{o.ns}, nil
}

func (o oneNamespace) ListNames(context.Context) ([]string, error) {
	return []string{o.ns.GetName()}, nil
}

func (o oneNamespace) Put(context.Context, *orkstackv1.Namespace, artifacts.PutConditions) (*orkstackv1.Namespace, error) {
	return nil, errors.New("oneNamespace: reads only")
}

func (o oneNamespace) Delete(context.Context, string) error {
	return errors.New("oneNamespace: reads only")
}

// TestApplyOrkflow_SecretRefusalCarriesItsFieldPath: apply and validate are
// one preparation, and the refusal apply returns is the same field-anchored
// error validate turns into an issue.
func TestApplyOrkflow_SecretRefusalCarriesItsFieldPath(t *testing.T) {
	orkflows := engine.NewMemoryOrkflowStore()
	svc := NewService(Config{
		Orkflows:   orkflows,
		Namespaces: oneNamespace{&orkstackv1.Namespace{Name: "sandbox", AllowedSecrets: []string{"env:SANDBOX_*"}}},
	})
	_, err := svc.ApplyOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Namespace: "sandbox",
		Yaml: `
apiVersion: orkstack/v1
kind: Orkflow
name: exfil
version: "1"
tasks:
  - id: warm
    uses: shell
    with: {command: "true"}
  - id: grab
    uses: shell
    secrets:
      - name: token
        source: env:SANDBOX_TOKEN
      - name: db
        source: env:PROD_DB_PASSWORD
    with: {command: "true"}
`,
	}))
	var field *validationFieldError
	if !errors.As(err, &field) || field.Field != "tasks[1].secrets[1].source" {
		t.Fatalf("ApplyOrkflow = %v, want a validationFieldError on tasks[1].secrets[1].source", err)
	}
	var refused *engine.SecretNotAllowedError
	if !errors.As(err, &refused) {
		t.Fatalf("ApplyOrkflow = %v, want it to wrap the SecretNotAllowedError", err)
	}
	if list, _ := orkflows.List(context.Background(), "sandbox"); len(list) != 0 {
		t.Fatalf("a refused apply stored %d definitions", len(list))
	}
}
